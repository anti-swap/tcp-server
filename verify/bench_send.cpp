// verify/bench_send.cpp -- round-trip cost of the reply path, in microseconds.
//
//   bench_send <messages> <payloadBytes>
//
// One connection, one frame in flight at a time: the client sends a DATA frame,
// the handler echoes it back with sendTo(), the client waits for the reply and
// the next frame goes out. That serialises the two directions, so the number is
// latency rather than throughput -- which is the one the design decisions in
// this file actually move. sendTo() from a handler thread crosses a queue and
// an eventfd today; if that ever gains an owner-thread fast path, this is where
// it shows up.
//
// Reported: p50 and p99 in microseconds, plus the achieved rate. p99 is the
// point of the exercise: a mean hides exactly the wakeup tail that a queue-and-
// eventfd hand-off produces.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::FRAME_HEADER_SIZE;
using industrial::MultiReactorServer;

namespace {

std::atomic<MultiReactorServer*> gServer{nullptr};
std::atomic<int> gPayload{56};

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

bool readExactly(int fd, void* p, size_t n) {
    uint8_t* b = static_cast<uint8_t*>(p);
    size_t off = 0;
    while (off < n) {
        const ssize_t k = ::recv(fd, b + off, n - off, 0);
        if (k <= 0) {
            if (k < 0 && errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(k);
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    const int messages = argc > 1 ? std::atoi(argv[1]) : 2000;
    gPayload.store(argc > 2 ? std::atoi(argv[2]) : 56);

    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = 0;
    cfg.numReactors   = 1;
    cfg.tickMs        = 500;
    cfg.idleTimeoutMs = 600000;

    MultiReactorServer server(cfg);
    gServer.store(&server);

    // Echo: the point is to pay for a reply, not to compute one.
    server.setMessageHandler([](uint64_t id, const uint8_t* p, size_t n) {
        MultiReactorServer* s = gServer.load();
        if (s) (void)s->sendTo(id, p, n);
    });
    if (!server.start()) return 2;

    const int fd = connectTo(server.port());
    if (fd < 0) { server.stop(); return 2; }

    // Warm up: the first exchange pays for the connection being registered and
    // for the page faults behind both buffers, and would otherwise land in p99.
    // The payload must be as long as the header claims: a shorter literal with a
    // longer declared length is a read past its end, and the bytes after it get
    // parsed as the next frame's header.
    std::vector<uint8_t> payload(static_cast<size_t>(gPayload.load()), 0x5a);
    std::vector<uint8_t> frame(FRAME_HEADER_SIZE + payload.size());
    std::vector<uint8_t> reply(frame.size());
    for (int i = 0; i < 100; ++i) {
        industrial::encodeFrameInto(frame.data(), FrameType::DATA,
                                    payload.data(), payload.size());
        if (::send(fd, frame.data(), frame.size(), MSG_NOSIGNAL) !=
            static_cast<ssize_t>(frame.size())) break;
        if (!readExactly(fd, reply.data(), frame.size())) break;
    }

    std::vector<uint64_t> ns;
    ns.reserve(static_cast<size_t>(messages));

    for (int i = 0; i < messages; ++i) {
        industrial::encodeFrameInto(frame.data(), FrameType::DATA,
                                    payload.data(), payload.size());
        const auto t0 = std::chrono::steady_clock::now();
        if (::send(fd, frame.data(), frame.size(), MSG_NOSIGNAL) !=
            static_cast<ssize_t>(frame.size())) break;
        if (!readExactly(fd, reply.data(), frame.size())) break;
        const auto t1 = std::chrono::steady_clock::now();
        ns.push_back(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()));
    }

    ::close(fd);
    server.stop();

    if (ns.size() < 100) {
        std::printf("send messages=%zu TOO FEW\n", ns.size());
        return 1;
    }
    std::sort(ns.begin(), ns.end());
    const double p50 = static_cast<double>(ns[ns.size() / 2]) / 1000.0;
    const double p99 = static_cast<double>(ns[ns.size() * 99 / 100]) / 1000.0;
    const double mean = static_cast<double>(
        std::accumulate(ns.begin(), ns.end(), uint64_t{0})) / ns.size() / 1000.0;
    std::printf("send messages=%zu payload=%d p50_us=%.1f p99_us=%.1f mean_us=%.1f\n",
                ns.size(), gPayload.load(), p50, p99, mean);
    return 0;
}
