#include "../net/netcompat.h"
#include "dect2/tx/tx_udpsink.h"
#include "dect2/common/thread_name.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <thread>
#include <vector>

namespace dect2 {

namespace {

bool resolveDst(const std::string& host, int port, sockaddr_in& out, std::string& err) {
    memset(&out, 0, sizeof(out));
    out.sin_family = AF_INET;
    out.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host.c_str(), &out.sin_addr) == 1) {
        return true;
    }
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* res = nullptr;
    const std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0 || !res) {
        err = "cannot resolve UDP destination host \"" + host + "\"";
        if (res) freeaddrinfo(res);
        return false;
    }
    memcpy(&out, res->ai_addr, sizeof(sockaddr_in));
    freeaddrinfo(res);
    return true;
}

} // namespace

struct UdpTxSink::Impl {
    UdpTxConfig cfg;
    double rate = 2e6;
    sock_t sock = kBadSock;
    sockaddr_in dst{};
    std::atomic<uint64_t> samples{0}, packets{0}, bytes{0}, sendErrors{0};
    std::chrono::steady_clock::time_point next;
    bool started = false;
    std::vector<uint8_t> packBuf;

    ~Impl() {
        if (sock != kBadSock) {
            sockClose(sock);
            sock = kBadSock;
        }
    }
};

UdpTxSink::UdpTxSink(const UdpTxConfig& c) : p_(std::make_unique<Impl>()) {
    p_->cfg = c;
    if (p_->cfg.payloadSize <= 0) p_->cfg.payloadSize = 1472;
}

UdpTxSink::~UdpTxSink() {
    close();
}

bool UdpTxSink::open(const TxTune& t, std::string& err) {
    close();
    netInit();

    if (t.sampleRate > 0) p_->rate = t.sampleRate;
    if (p_->rate <= 0) p_->rate = 2e6;

    if (!resolveDst(p_->cfg.host.empty() ? "127.0.0.1" : p_->cfg.host, p_->cfg.port, p_->dst, err)) {
        return false;
    }

    p_->sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (p_->sock == kBadSock) {
        err = "failed to create UDP socket";
        return false;
    }

    int one = 1;
    setsockopt(p_->sock, SOL_SOCKET, SO_BROADCAST, (const char*)&one, sizeof(one));

    // Increase socket send buffer to 4 MB to minimize packet drops when system is under transient load
    int sndbuf = 4 << 20;
    setsockopt(p_->sock, SOL_SOCKET, SO_SNDBUF, (const char*)&sndbuf, sizeof(sndbuf));

    p_->samples = 0;
    p_->packets = 0;
    p_->bytes = 0;
    p_->sendErrors = 0;
    p_->started = false;

    if (p_->cfg.counters) {
        p_->cfg.counters->samples = 0;
        p_->cfg.counters->packets = 0;
        p_->cfg.counters->bytes = 0;
        p_->cfg.counters->sendErrors = 0;
        p_->cfg.counters->rateHz = p_->rate;
    }

    return true;
}

bool UdpTxSink::write(const cf32* x, size_t n, std::string& err) {
    if (p_->sock == kBadSock) {
        err = "UDP socket is closed";
        return false;
    }
    if (!x || n == 0) return true;

    const auto now = std::chrono::steady_clock::now();
    if (!p_->started) {
        p_->next = now;
        p_->started = true;
    } else if (p_->next < now - std::chrono::milliseconds(200)) {
        // If execution stalled or resumed after a pause, advance clock to avoid huge burst
        p_->next = now;
    }

    const size_t bps = txIqBytesPerSample(p_->cfg.format);
    const size_t maxBytes = (size_t)std::max(16, p_->cfg.payloadSize);
    // Align packet size to sample boundaries
    const size_t spd = std::max<size_t>(1, maxBytes / bps);

    for (size_t off = 0; off < n; off += spd) {
        const size_t chunkSamples = std::min(spd, n - off);
        const size_t chunkBytes = chunkSamples * bps;

        if (p_->packBuf.size() < chunkBytes) {
            p_->packBuf.resize(chunkBytes);
        }

        txIqPack(x + off, chunkSamples, p_->cfg.format, p_->packBuf.data());

        const long long r = sockSendTo(p_->sock, p_->packBuf.data(), chunkBytes, p_->dst);
        if (r < 0) {
            p_->sendErrors++;
            if (p_->cfg.counters) p_->cfg.counters->sendErrors++;
        } else {
            p_->packets++;
            p_->bytes += (uint64_t)r;
            if (p_->cfg.counters) {
                p_->cfg.counters->packets++;
                p_->cfg.counters->bytes += (uint64_t)r;
            }
        }
    }

    p_->samples += n;
    if (p_->cfg.counters) {
        p_->cfg.counters->samples = p_->samples.load();
        p_->cfg.counters->rateHz = p_->rate;
    }

    // Pace according to sample rate
    p_->next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
        std::chrono::duration<double>((double)n / p_->rate));
    std::this_thread::sleep_until(p_->next);

    return true;
}

bool UdpTxSink::retune(const TxTune& t, std::string&) {
    if (t.sampleRate > 0) {
        p_->rate = t.sampleRate;
        if (p_->cfg.counters) p_->cfg.counters->rateHz = p_->rate;
    }
    return true;
}

void UdpTxSink::close() {
    if (p_->sock != kBadSock) {
        sockClose(p_->sock);
        p_->sock = kBadSock;
    }
    p_->started = false;
}

double UdpTxSink::sampleRate() const {
    return p_->rate;
}

uint64_t UdpTxSink::samplesSent() const {
    return p_->samples.load();
}

uint64_t UdpTxSink::packetsSent() const {
    return p_->packets.load();
}

uint64_t UdpTxSink::bytesSent() const {
    return p_->bytes.load();
}

std::unique_ptr<UdpTxSink> makeUdpTxSink(const UdpTxConfig& c) {
    return std::make_unique<UdpTxSink>(c);
}

} // namespace dect2
