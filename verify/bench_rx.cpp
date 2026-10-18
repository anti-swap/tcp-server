// verify/bench_rx.cpp -- how many bytes a reactor can take off the socket and
// hand to a no-op handler, and whether more reactors buy more of it.
//
//   bench_rx <reactors> <clientThreads> <seconds>
//
// The client runs in the same process on purpose. A separate process would make
// the absolute number look better and tell us nothing about which side was the
// limit; keeping them together means the number is a floor for both halves
// combined, and the reactor-to-reactor ratio -- which is what this benchmark is
// actually for -- survives the noise.
//
// Frames are 64 bytes and are sent 64 at a time, so the syscall count does not
// dominate what is being measured.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

int connectTo(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

}  // namespace

int main(int argc, char** argv) {
    const int reactors = argc > 1 ? std::atoi(argv[1]) : 1;
    const int threads  = argc > 2 ? std::atoi(argv[2]) : 1;
    const int seconds  = argc > 3 ? std::atoi(argv[3]) : 3;

    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = 0;
    cfg.numReactors   = reactors;
    cfg.tickMs        = 500;
    cfg.idleTimeoutMs = 600000;

    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) return 2;
    const uint16_t port = server.port();

    // The payload has to be as long as the length in the header says it is.
    // Encoding a 56-byte length out of a 2-byte string literal is a read past
    // the end of it -- and the server then parses the slack as the next frame's
    // header and closes the connection on a magic mismatch.
    const std::vector<uint8_t> payload(56, 0x5a);
    std::vector<uint8_t> frame(64);
    industrial::encodeFrameInto(frame.data(), FrameType::DATA,
                                payload.data(), payload.size());
    std::vector<uint8_t> batch;
    for (int i = 0; i < 64; ++i) batch.insert(batch.end(), frame.begin(), frame.end());

    std::atomic<bool> go{false};
    std::atomic<bool> stop{false};
    std::atomic<long long> sent{0};

    std::vector<std::thread> clients;
    for (int t = 0; t < threads; ++t) {
        clients.emplace_back([&] {
            const int fd = connectTo(port);
            if (fd < 0) return;
            while (!go.load()) {}
            while (!stop.load()) {
                const ssize_t k = ::send(fd, batch.data(), batch.size(), MSG_NOSIGNAL);
                if (k <= 0) break;
                sent.fetch_add(k);
            }
            ::close(fd);
        });
    }

    // Let the connections register before the clock starts, or the first batch
    // measures accept rather than ingest.
    for (int i = 0; i < 200 && server.connectionCount() < clients.size(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    const uint64_t before = server.stats().bytesRecv.load();
    go.store(true);
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    stop.store(true);
    for (auto& c : clients) c.join();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    const uint64_t after = server.stats().bytesRecv.load();

    server.stop();

    const double mbps = static_cast<double>(after - before) / (1024.0 * 1024.0) /
                        static_cast<double>(seconds);
    const double kfps = static_cast<double>(after - before) / 64.0 /
                        static_cast<double>(seconds) / 1000.0;
    std::printf("ingest reactors=%d clients=%d mb_per_s=%.1f kframes_per_s=%.0f\n",
                reactors, threads, mbps, kfps);
    return 0;
}
