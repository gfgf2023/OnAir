// GNU Radio UDP Sink: transmit baseband IQ over UDP to a GNU Radio UDP Source block.
// Packs complex samples into the chosen format (cf32, cs16, cs8, cu8), partitions them into
// datagrams respecting the maximum payload size (default 1472 bytes for Ethernet MTU), and
// paces transmission at the selected sample rate using monotonic time.
#pragma once
#include "dect2/source/source.h"
#include "dect2/tx/tx_iqstream.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace dect2 {

struct UdpTxCounters {
    std::atomic<uint64_t> samples{0};
    std::atomic<uint64_t> packets{0};
    std::atomic<uint64_t> bytes{0};
    std::atomic<uint64_t> sendErrors{0};
    std::atomic<double> rateHz{0};
    double seconds() const {
        const double r = rateHz.load();
        return r > 0 ? (double)samples.load() / r : 0.0;
    }
};

struct UdpTxConfig {
    std::string host = "127.0.0.1";
    int port = 1234;
    TxIqFormat format = TxIqFormat::Cf32;
    int payloadSize = 1472;   // bytes per UDP datagram (e.g. 1472 for standard 1500 MTU)
    std::shared_ptr<UdpTxCounters> counters;
};

class UdpTxSink : public TxSink {
public:
    explicit UdpTxSink(const UdpTxConfig& c);
    ~UdpTxSink() override;

    bool open(const TxTune& t, std::string& err) override;
    bool write(const cf32* x, size_t n, std::string& err) override;
    bool retune(const TxTune& t, std::string& err) override;
    void close() override;

    double sampleRate() const override;
    bool realtime() const override { return true; }
    std::string name() const override { return "GNU Radio UDP"; }
    bool takesIq() const override { return true; }

    uint64_t samplesSent() const;
    uint64_t packetsSent() const;
    uint64_t bytesSent() const;

private:
    struct Impl;
    std::unique_ptr<Impl> p_;
};

std::unique_ptr<UdpTxSink> makeUdpTxSink(const UdpTxConfig& c);

} // namespace dect2
