// verify/evidence/probe_fin_mod_failure.cpp
//
// Does the failed MOD in finishPeerClosed() really cost nothing?
//
// The code faces one situation: the peer has sent FIN, we still owe it bytes
// (wbuf non-empty), so the connection has to be parked with EPOLLOUT only --
// dropping EPOLLIN, because a FIN makes the socket readable for the rest of its
// life and level-triggered read interest would then refire on every epoll_wait.
// That MOD can fail. On failure the code does this:
//
//     server_->statsRef().epollCtlErrors.fetch_add(1, ...);
//     // A failed MOD keeps EPOLLIN armed: the drain spins, nothing breaks.
//
// "the drain spins" is the whole problem, and "nothing breaks" is the part that
// is wrong: a spin is not free, and this one is unbounded in time -- the only
// deadline left on a connection that is not closing is idleTimeoutMs (120s by
// default). Every iteration re-runs recv() -> 0 -> finishPeerClosed() -> the
// same failing epoll_ctl.
//
// Making epoll_ctl fail on purpose is the only way to see it, so this probe
// wraps it at link time and fails exactly one shape of call: EPOLL_CTL_MOD with
// EPOLLOUT alone set -- i.e. the park in finishPeerClosed. Nothing else in the
// file issues that shape (armWrite is IN|OUT, disarmWrite is IN only), so the
// filter cannot catch a healthy call by accident.
//
// Three runs, same workload (16MB owed to a peer with a 2KB receive buffer that
// then sends FIN and keeps its socket open):
//
//   pass    the MOD succeeds      -> eofReads delta ~0, the parked design
//   ENOMEM  the MOD fails         -> eofReads delta counts the spin
//   ENOENT  the MOD fails and the registration really is gone (DEL first)
//                                 -> no events at all: the drain freezes
//
// The second is the reviewer's claim, measured. The third is what the suggested
// ENOENT->ADD recovery is for: without it a "registration set out of sync"
// failure parks the connection into total silence.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1 -Wl,--wrap=epoll_ctl
//       verify/evidence/probe_fin_mod_failure.cpp -o probe_fin_mod_failure
//   ./probe_fin_mod_failure
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <functional>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

// 0 = let every call through, 1 = ENOMEM, 2 = ENOENT with the fd really removed.
int  g_failMode    = 0;
long g_parkAttempts = 0;
long g_parkFailures = 0;

}  // namespace

extern "C" int __real_epoll_ctl(int epfd, int op, int fd, struct epoll_event* ev);

extern "C" int __wrap_epoll_ctl(int epfd, int op, int fd, struct epoll_event* ev) {
    const bool isPark = g_failMode != 0 && op == EPOLL_CTL_MOD && ev != nullptr &&
                        (ev->events & EPOLLOUT) && !(ev->events & EPOLLIN);
    if (isPark) {
        ++g_parkAttempts;
        ++g_parkFailures;
        if (g_failMode == 1) {
            errno = ENOMEM;
            return -1;
        }
        // ENOENT means "this fd is not in the interest list". Reproduce the
        // state the error code describes, not just the code: without the DEL the
        // socket would keep firing and this arm would look like the ENOMEM one.
        __real_epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
        errno = ENOENT;
        return -1;
    }
    return __real_epoll_ctl(epfd, op, fd, ev);
}

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

int connectWithSmallRcvbuf(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    // Before connect(), so the advertised window is small from the first SYN.
    // A tiny receive buffer is what guarantees the server still owes megabytes
    // when the FIN arrives.
    int rcv = 2048;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
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

bool sendAll(int fd, const std::vector<uint8_t>& f) {
    size_t off = 0;
    while (off < f.size()) {
        ssize_t n = ::send(fd, f.data() + off, f.size() - off, MSG_NOSIGNAL);
        if (n <= 0) return false;
        off += static_cast<size_t>(n);
    }
    return true;
}

struct Sample {
    unsigned long long eof = 0, ctl = 0, sent = 0, dropped = 0;
    size_t open = 0;
};

// Whole-process CPU time: the client half of this probe only sleeps, so during
// the measurement window this is the reactor thread's bill.
double cpuSecs() {
    struct timespec ts{};
    ::clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) / 1e9;
}

Sample take(MultiReactorServer& s) {
    const auto& st = s.stats();
    Sample r;
    r.eof     = st.eofReads.load();
    r.ctl     = st.epollCtlErrors.load();
    r.sent    = st.bytesSent.load();
    r.dropped = st.bytesDropped.load();
    r.open    = s.connectionCount();
    return r;
}

const char* modeName(int m) {
    switch (m) {
        case 0: return "no failure (pass)";
        case 1: return "park MOD fails ENOMEM";
        default: return "park MOD fails ENOENT";
    }
}

// 16MB of replies to a 2KB-window peer: after 250ms the drain is long stalled
// and wbuf still holds most of it. Then the peer sends FIN and stays.
//
// watchTail additionally waits for the connection to disappear and reports what
// ended it. With idleMs/lingerMs shortened this shows the whole tail of the
// failure: nothing at the MOD gives up, the timers do, seconds later.
void run(int mode, int windowMs = 400, int idleMs = 60000, int lingerMs = 5000,
         bool watchTail = false, bool drainAtEnd = false) {
    MultiReactorServer::Config cfg;
    cfg.bindAddr        = "127.0.0.1";
    cfg.port            = pickPort();
    cfg.numReactors     = 1;
    cfg.tickMs          = 100;
    cfg.idleTimeoutMs   = idleMs;
    cfg.closeLingerMs   = lingerMs;
    cfg.wbufCapacity    = 64u * 1024 * 1024;
    cfg.maxPendingBytes = 64u * 1024 * 1024;
    cfg.writeBudgetBytes = 1024 * 1024;

    MultiReactorServer server(cfg);
    server.setMessageHandler([&](uint64_t id, const uint8_t*, size_t) {
        std::vector<uint8_t> payload(32 * 1024, 'x');
        for (int i = 0; i < 512; ++i)   // 16MB, all queued from one request
            server.sendTo(id, payload.data(), payload.size());
    });
    if (!server.start()) {
        std::printf("%-28s start failed\n", modeName(mode));
        return;
    }

    int fd = connectWithSmallRcvbuf(cfg.port);
    if (fd < 0) {
        std::printf("%-28s connect failed\n", modeName(mode));
        server.stop();
        return;
    }
    auto req = encode(FrameType::DATA, "answer me", 9);
    sendAll(fd, req);

    // Let the handler queue everything and the drain hit EAGAIN for good.
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    const Sample before = take(server);

    g_failMode = mode;
    const double cpu0 = cpuSecs();
    ::shutdown(fd, SHUT_WR);   // FIN with megabytes still owed to this peer
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(windowMs));
    const Sample after = take(server);
    const double cpu1 = cpuSecs();

    const double secs =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    std::printf("%-28s w=%-4d eofReads +%-7llu epollCtlErr +%-7llu bytesSent +%-6llu "
                "aborted %llu open %zu  MOD %ld (%.0f/s)  cpu %.0f%%",
                modeName(mode), windowMs,
                after.eof - before.eof,
                after.ctl - before.ctl,
                (unsigned long long)after.sent - before.sent,
                (unsigned long long)server.stats().aborted.load(),
                after.open,
                g_parkAttempts,
                g_parkAttempts / secs,
                100.0 * (cpu1 - cpu0) / secs);

    if (watchTail) {
        // Everything left is a deadline: idleTimeoutMs for a peer that owes
        // nothing, then closeLingerMs for the resulting close.
        auto t1 = std::chrono::steady_clock::now();
        while (server.connectionCount() != 0 &&
               std::chrono::duration<double>(std::chrono::steady_clock::now() - t1).count() < 10.0)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        const auto& st = server.stats();
        std::printf("\n%-28s ended after %.2fs: idleTimeouts %llu aborted %llu "
                    "bytesDropped %llu open %zu, MOD %ld total",
                    "", std::chrono::duration<double>(
                            std::chrono::steady_clock::now() - t0).count(),
                    (unsigned long long)st.idleTimeouts.load(),
                    (unsigned long long)st.aborted.load(),
                    (unsigned long long)st.bytesDropped.load(),
                    server.connectionCount(), g_parkAttempts);
    }
    std::printf("\n");

    // The peer finally reads. This is the endgame the park exists for: a peer
    // that comes back must get everything it was owed, and the socket must end
    // with a FIN, not a reset. A park that silently lost its registration
    // (ENOENT) fails here -- nothing ever wakes it up; a park that spun (ENOMEM)
    // fails earlier, by being torn down at the MOD.
    if (drainAtEnd) {
        struct timeval tv{};
        tv.tv_sec = 3;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        std::vector<char> buf(64 * 1024);
        size_t   got     = 0;
        int      lastErr = 0;
        for (;;) {
            ssize_t n = ::recv(fd, buf.data(), buf.size(), 0);
            if (n > 0) { got += static_cast<size_t>(n); continue; }
            if (n < 0) lastErr = errno;
            break;                       // 0 = server's FIN, <0 = reset/timeout
        }
        for (int i = 0; i < 100 && server.connectionCount() != 0; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        const auto& st = server.stats();
        std::printf("\n%-28s peer read back: got %zu of 16781312 bytes, "
                    "aborted %llu dropped %llu open %zu (%s)",
                    "", got,
                    (unsigned long long)st.aborted.load(),
                    (unsigned long long)st.bytesDropped.load(),
                    server.connectionCount(),
                    lastErr ? std::strerror(lastErr) : "server FIN");
    }

    // Whatever mode, keep the peer's unread megabytes from turning this into the
    // abrupt-close case P1-2 already covers.
    ::close(fd);
    server.stop();
    g_failMode = 0;
    g_parkAttempts = 0;
    g_parkFailures = 0;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("finishPeerClosed's park MOD, when it fails (16MB owed, peer FIN'd,\n"
                "peer keeps its socket open)\n\n");
    run(0, 400, 60000, 5000, false, true);         // healthy park, for contrast
    run(1, 400, 60000, 5000, false, true);         // ENOMEM: the spin
    run(2, 400, 60000, 5000, false, true);         // ENOENT: total silence
    // Same ENOMEM failure, but watched to the end: idle 1.5s, linger 0.5s.
    run(1, 2500, 1500, 500, true);
    std::printf("\neofReads is the file's own busy-poll probe (see Stats): every\n"
                "spin iteration is exactly one recv()==0.\n");
    return 0;
}
