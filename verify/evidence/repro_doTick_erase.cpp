// verify/evidence/repro_doTick_erase.cpp
//
// Evidence for round-3 open item #2: doTick()'s sweep over conns_ calls
// closeConn(), and a connection that has already sent its FIN is torn down
// synchronously -- teardownClosed() erases from conns_ while the range-for is
// still walking it. Today that state cannot arise (finSent implies closing),
// but the safety of the loop rests on a fact five functions away.
//
// This is not part of the suite. It is the demonstration that the hazard is
// real and that the fix removes it, run under AddressSanitizer where erasing
// mid-iteration is a heap-use-after-free instead of silence:
//
//   - with inject_fin_without_closing.py applied to a tree whose doTick still
//     erases in-loop: ASan reports a use-after-free inside doTick
//   - with the same injection applied to the fixed doTick: clean
//
// It builds 24 connections that go idle, get closed gracefully (FIN sent, then
// parked waiting for the peer's FIN), and simply sit there -- one deadline
// (closeLingerMs) away from being erased -- through several hundred sweeps.
//
// Build and run (Linux only, from the repo root; see check_doTick_invariant.sh,
// which drives both sides of the comparison):
//   g++ -std=c++17 -Wall -Wextra -pthread -g -fsanitize=address,undefined -o repro_doTick_erase verify/evidence/repro_doTick_erase.cpp
//   ./repro_doTick_erase
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

#include "../tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

uint16_t pickPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    ::bind(fd, (struct sockaddr*)&sa, sizeof(sa));
    socklen_t len = sizeof(sa);
    ::getsockname(fd, (struct sockaddr*)&sa, &len);
    uint16_t port = ntohs(sa.sin_port);
    ::close(fd);
    return port;
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

std::vector<uint8_t> encode(FrameType type, const void* payload, size_t len) {
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + len);
    industrial::encodeFrameInto(f.data(), type, payload, len);
    return f;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    MultiReactorServer::Config cfg;
    cfg.port            = pickPort();
    cfg.bindAddr        = "127.0.0.1";
    cfg.numReactors     = 1;
    cfg.idleTimeoutMs   = 120;    // every connection is overdue within ~140ms
    cfg.tickMs          = 20;     // hundreds of sweeps over the parked set
    cfg.closeLingerMs   = 4000;   // parked ones stay in conns_ for the whole run

    MultiReactorServer server(cfg);
    // No reply: nothing keeps these connections alive except the peer's half of
    // the close, which never comes.
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) {
        std::printf("start failed\n");
        return 2;
    }

    std::vector<int> fds;
    for (int i = 0; i < 24; ++i) {
        int fd = connectTo(cfg.port);
        if (fd < 0) {
            std::printf("connect %d failed: errno=%d (%s)\n", i, errno, std::strerror(errno));
            server.stop();
            return 2;
        }
        auto req = encode(FrameType::DATA, "x", 1);
        if (::send(fd, req.data(), req.size(), 0) < 0) {
            std::printf("send %d failed: errno=%d (%s)\n", i, errno, std::strerror(errno));
            ::close(fd);
            server.stop();
            return 2;
        }
        fds.push_back(fd);
    }

    // Every connection is idle-timed-out, FIN'd, and parked inside this window;
    // doTick sweeps the map the whole time.
    std::this_thread::sleep_for(std::chrono::milliseconds(2500));

    const auto& st = server.stats();
    std::printf("accepted=%llu closed=%llu aborted=%llu idleTimeouts=%llu stillOpen=%zu\n",
                (unsigned long long)st.accepted.load(),
                (unsigned long long)st.closed.load(),
                (unsigned long long)st.aborted.load(),
                (unsigned long long)st.idleTimeouts.load(),
                server.connectionCount());

    server.stop();
    for (int fd : fds) ::close(fd);
    std::printf("reached the end without tripping the sanitizer\n");
    return 0;
}
