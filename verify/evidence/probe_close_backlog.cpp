// verify/evidence/probe_close_backlog.cpp
//
// closeConnection() promises to serve the peer's pending input and then FIN.
// drainInputThenClose() gets there by calling handleRead() once, and deferring
// the park by one queue round so the replies land first. Neither of those two
// knows what handleRead actually guarantees: it stops after kFrameBudget = 256
// frames or kReadBudget = 64KB, whichever comes first, and schedules itself for
// more. So "the input has been read" is only true up to that budget -- and the
// close does not come back for a second pass, because waitingOnReplies defers
// exactly once and then parks.
//
// The question this answers is whether a peer backlog bigger than one read
// budget is answered before the FIN, or FIN'd over. Nothing in the suite sends
// a burst that large and then closes: case A is one frame, case E is one request
// with a huge answer.
//
// Workload: N small frames in one write, handler echoes each and asks for the
// close on the first one ("answer, then hang up"). The client then reads to EOF
// and counts. A healthy close answers all N.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1
//       verify/evidence/probe_close_backlog.cpp -o probe_close_backlog
//   ./probe_close_backlog
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

uint16_t pickPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (::bind(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return 0;
    }
    socklen_t sl = sizeof(sa);
    ::getsockname(fd, (struct sockaddr*)&sa, &sl);
    ::close(fd);
    return ntohs(sa.sin_port);
}

int connectTo(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
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

std::vector<uint8_t> encode(FrameType type, const void* p, size_t n) {
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + n);
    industrial::encodeFrameInto(f.data(), type, p, n);
    return f;
}

void run(int n) {
    MultiReactorServer::Config cfg;
    cfg.bindAddr        = "127.0.0.1";
    cfg.port            = pickPort();
    cfg.numReactors     = 1;
    cfg.closeLingerMs   = 3000;
    cfg.idleTimeoutMs   = 60000;
    cfg.wbufCapacity    = 32u * 1024 * 1024;
    cfg.maxPendingBytes = 32u * 1024 * 1024;

    std::atomic<long> dispatched{0};
    std::atomic<bool> closeAsked{false};
    MultiReactorServer server(cfg);
    server.setMessageHandler([&](uint64_t id, const uint8_t* p, size_t len) {
        dispatched.fetch_add(1);
        server.sendTo(id, p, len);          // echo
        if (!closeAsked.exchange(true)) server.closeConnection(id);
    });
    if (!server.start()) {
        std::printf("N=%-5d start failed\n", n);
        return;
    }

    int fd = connectTo(cfg.port);
    if (fd < 0) {
        std::printf("N=%-5d connect failed\n", n);
        server.stop();
        return;
    }

    // One burst: every frame in a single write, so the server's first read sees
    // the whole backlog at once.
    std::string payload(32, 'q');
    std::vector<uint8_t> burst;
    for (int i = 0; i < n; ++i) {
        auto f = encode(FrameType::DATA, payload.data(), payload.size());
        burst.insert(burst.end(), f.begin(), f.end());
    }
    size_t off = 0;
    while (off < burst.size()) {
        ssize_t k = ::send(fd, burst.data() + off, burst.size() - off, MSG_NOSIGNAL);
        if (k <= 0) break;
        off += static_cast<size_t>(k);
    }

    // Read to EOF: everything the server answered, then the FIN.
    std::vector<uint8_t> in;
    in.reserve(burst.size() + 4096);
    uint8_t buf[64 * 1024];
    for (;;) {
        ssize_t k = ::recv(fd, buf, sizeof(buf), 0);
        if (k <= 0) break;
        in.insert(in.end(), buf, buf + k);
    }
    const size_t frameSize = industrial::FRAME_HEADER_SIZE + payload.size();
    const long   echoes    = static_cast<long>(in.size() / frameSize);

    const auto& st = server.stats();
    std::printf("N=%-5d answered %-5ld dispatched %-5ld  queueDrops %-5llu "
                "bytesDropped %-9llu framesSent %-5llu aborted %llu closed %llu\n",
                n, echoes, dispatched.load(),
                (unsigned long long)st.queueDrops.load(),
                (unsigned long long)st.bytesDropped.load(),
                (unsigned long long)st.framesSent.load(),
                (unsigned long long)st.aborted.load(),
                (unsigned long long)st.closed.load());

    ::close(fd);
    server.stop();
}

// The relaxation has to stay bounded: "at most one round" existed so that a peer
// that keeps sending cannot hold the close open. This keeps sending after the
// close was requested and measures how long the close takes anyway. It is a
// guard, not a red/green case: the unfixed code parks at once, so it ends fast
// for the wrong reason -- what it protects against is a future version that
// defers while input remains with no budget behind it.
void runHostile() {
    MultiReactorServer::Config cfg;
    cfg.bindAddr        = "127.0.0.1";
    cfg.port            = pickPort();
    cfg.numReactors     = 1;
    cfg.closeLingerMs   = 3000;
    cfg.idleTimeoutMs   = 60000;
    cfg.wbufCapacity    = 32u * 1024 * 1024;
    cfg.maxPendingBytes = 32u * 1024 * 1024;

    std::atomic<bool> closeAsked{false};
    MultiReactorServer server(cfg);
    server.setMessageHandler([&](uint64_t id, const uint8_t* p, size_t len) {
        server.sendTo(id, p, len);
        if (!closeAsked.exchange(true)) server.closeConnection(id);
    });
    if (!server.start()) {
        std::printf("hostile   start failed\n");
        return;
    }
    int fd = connectTo(cfg.port);
    if (fd < 0) {
        std::printf("hostile   connect failed\n");
        server.stop();
        return;
    }

    std::string payload(32, 'q');
    auto burst = [&](int n) {
        std::vector<uint8_t> b;
        for (int i = 0; i < n; ++i) {
            auto f = encode(FrameType::DATA, payload.data(), payload.size());
            b.insert(b.end(), f.begin(), f.end());
        }
        size_t off = 0;
        while (off < b.size()) {
            ssize_t k = ::send(fd, b.data() + off, b.size() - off, MSG_NOSIGNAL);
            if (k <= 0) return;
            off += static_cast<size_t>(k);
        }
    };

    const auto t0 = std::chrono::steady_clock::now();
    burst(2000);                       // backlog, and the close is asked on frame 1
    uint8_t buf[64 * 1024];
    bool fin = false;
    int  rounds = 0;
    while (std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count() < 3.0) {
        burst(256);                    // keep sending: the peer never goes idle
        ++rounds;
        for (;;) {                     // drain without blocking: 0 means FIN
            ssize_t k = ::recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
            if (k > 0) continue;
            if (k == 0) fin = true;
            break;
        }
        if (fin) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const double secs =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    std::printf("hostile   FIN after %.3fs (%d more bursts sent), closed %llu "
                "queueDrops %llu\n",
                secs, rounds,
                (unsigned long long)server.stats().closed.load(),
                (unsigned long long)server.stats().queueDrops.load());
    ::close(fd);
    server.stop();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("closeConnection() with a backlog: is everything answered before "
                "the FIN?\n(handleRead stops at 256 frames / 64KB per call)\n\n");
    run(100);
    run(256);
    run(300);
    run(600);
    run(1200);
    std::printf("\n");
    runHostile();
    return 0;
}
