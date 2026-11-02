#include "../../core/src/net/netcompat.h"
#include "dect2/tx/tx_udpsink.h"
#include "../common/test_util.h"
#include <chrono>
#include <cmath>
#include <cstring>
#include <thread>
#include <vector>

using namespace dect2;

static sock_t bindUdp(int& boundPort) {
    netInit();
    const sock_t s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s == kBadSock) return kBadSock;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = 0;   // bind ephemeral port
    inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    if (bind(s, (sockaddr*)&a, sizeof(a)) != 0) {
        sockClose(s);
        return kBadSock;
    }
    socklen_t len = sizeof(a);
    if (getsockname(s, (sockaddr*)&a, &len) == 0) {
        boundPort = ntohs(a.sin_port);
    }
    return s;
}

static std::vector<cf32> testSignal(size_t n) {
    std::vector<cf32> x(n);
    for (size_t i = 0; i < n; i++) {
        x[i] = cf32((float)(0.7 * std::cos(0.04 * (double)i)),
                    (float)(-0.5 * std::sin(0.03 * (double)i)));
    }
    return x;
}

int main() {
    int port = 0;
    sock_t rx = bindUdp(port);
    CHECK(rx != kBadSock, "bind UDP receiver socket");
    CHECK(port > 0, "bound port %d > 0", port);

    // Test 1: cf32 format transmission
    {
        UdpTxConfig cfg;
        cfg.host = "127.0.0.1";
        cfg.port = port;
        cfg.format = TxIqFormat::Cf32;
        cfg.payloadSize = 1472; // 184 samples per datagram
        cfg.counters = std::make_shared<UdpTxCounters>();

        auto sink = makeUdpTxSink(cfg);
        CHECK(sink != nullptr, "sink created");
        CHECK(sink->name() == "GNU Radio UDP", "sink name");

        TxTune tune;
        tune.sampleRate = 500000; // 500 ksps
        std::string err;
        CHECK(sink->open(tune, err), "open sink: %s", err.c_str());
        CHECK(sink->sampleRate() == 500000, "sample rate set");

        const size_t nSamples = 368; // exactly 2 packets of 184 samples
        const auto x = testSignal(nSamples);

        CHECK(sink->write(x.data(), nSamples, err), "write cf32 samples: %s", err.c_str());
        CHECK(sink->samplesSent() == nSamples, "samplesSent == %zu", nSamples);
        CHECK(sink->packetsSent() == 2, "packetsSent == 2");
        CHECK(sink->bytesSent() == nSamples * 8, "bytesSent == %zu", nSamples * 8);

        // Receive datagrams
        std::vector<uint8_t> recvBuf(2048);
        for (int p = 0; p < 2; p++) {
            fd_set rd; FD_ZERO(&rd); FD_SET(rx, &rd);
            timeval tv{2, 0};
            int sel = select((int)rx + 1, &rd, nullptr, nullptr, &tv);
            CHECK(sel > 0, "packet %d received before timeout", p);

            long long r = sockRecv(rx, recvBuf.data(), recvBuf.size());
            CHECK(r == 184 * 8, "packet %d size: %lld", p, r);

            // Verify samples
            const auto* gotSamples = reinterpret_cast<const cf32*>(recvBuf.data());
            for (size_t i = 0; i < 184; i++) {
                CHECK(gotSamples[i] == x[p * 184 + i], "sample match at packet %d index %zu", p, i);
            }
        }

        sink->close();
    }

    // Test 2: cs16 format transmission & localhost resolution
    {
        UdpTxConfig cfg;
        cfg.host = "localhost";
        cfg.port = port;
        cfg.format = TxIqFormat::Cs16;
        cfg.payloadSize = 1024; // 1024 / 4 = 256 samples per datagram
        cfg.counters = std::make_shared<UdpTxCounters>();

        auto sink = makeUdpTxSink(cfg);
        TxTune tune;
        tune.sampleRate = 250000;
        std::string err;
        CHECK(sink->open(tune, err), "open cs16 sink: %s", err.c_str());

        const size_t nSamples = 256;
        const auto x = testSignal(nSamples);
        CHECK(sink->write(x.data(), nSamples, err), "write cs16: %s", err.c_str());

        fd_set rd; FD_ZERO(&rd); FD_SET(rx, &rd);
        timeval tv{2, 0};
        int sel = select((int)rx + 1, &rd, nullptr, nullptr, &tv);
        CHECK(sel > 0, "cs16 packet received");

        std::vector<uint8_t> recvBuf(2048);
        long long r = sockRecv(rx, recvBuf.data(), recvBuf.size());
        CHECK(r == 256 * 4, "cs16 packet size %lld == 1024", r);

        sink->close();
    }

    sockClose(rx);
    return testSummary("test_tx_udpsink");
}
