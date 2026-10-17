// verify/backpressure.cpp -- what one round of replies costs a single reactor.
//
// The task queue cap used to be a flat 8192, per reactor, and nothing said so.
// A broadcast is the shape that notices: every connection the server owns gets a
// reply queued on whichever reactor holds it, so the number that matters is not
// "how many connections does the server have" but "how many can land on one
// worker". With the default maxConnections of 10000 and one reactor, the first
// ~8192 subscribers got their message and the rest were counted as queueDrops --
// which reads like peers failing to drain, not like a constant.
//
// So this case fills one reactor's queue past the old cap and past nothing else:
//
//   - wbufCapacity and maxPendingBytes are both raised far above what the test
//     sends, so neither can be the thing that refuses a frame. A test whose
//     subject is one limit has to make the other limits unreachable, or it will
//     report whichever one tripped first and call it an answer.
//   - The worker is then held inside the open callback. A queue only fills if
//     nothing is draining it, and that is exactly what "the worker is busy doing
//     something else" means; letting it drain would turn the assertion into a
//     race between the producer and the consumer.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread verify/backpressure.cpp -o backpressure
//
// Exit code 0 = the fan-out fit.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "../tcp-server-mt.h"

using industrial::MultiReactorServer;
using industrial::SendResult;

namespace {

int gFailures = 0;

void expect(bool ok, const char* what) {
    std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what);
    if (!ok) ++gFailures;
}

bool waitFor(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

uint16_t pickPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
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
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    // Past the flat floor the cap used to be, comfortably inside what a single
    // reactor serving all of maxConnections should be able to queue.
    const int kFanOut = 9000;

    std::printf("A. one reactor has to be able to queue one round of replies\n");

    MultiReactorServer::Config cfg;
    cfg.bindAddr = "127.0.0.1";
    cfg.port = pickPort();
    // One worker, so every submission lands on the same queue and nothing gets
    // averaged away across reactors.
    cfg.numReactors = 1;
    // See the header: both raised until neither can be the limit being measured.
    cfg.maxPendingBytes = 8u << 20;
    cfg.wbufCapacity    = 8u << 20;

    MultiReactorServer server(cfg);

    std::atomic<bool>   release{false};
    std::atomic<uint64_t> connId{0};
    std::atomic<bool>    reachedCb{false};

    // Blocking the worker with a mutex plus a condition variable is the obvious
    // spelling and it is what this used to do. Not here: this machine's
    // ThreadSanitizer mis-models std::condition_variable::wait_for and reports
    // "double lock of a mutex" followed by a race on the flag guarded by that
    // mutex, with no server in the process at all -- see
    // verify/probe_tsan_condvar.sh. Suppressing it would leave the codebase
    // arguing with a sanitizer, so wait on an atomic with a deadline instead.
    // Same property being tested, nothing left to mis-model.
    server.setConnStateHandler([&](uint64_t id, const std::string&, bool up) {
        if (!up) return;
        connId.store(id, std::memory_order_release);
        reachedCb.store(true, std::memory_order_release);
        for (int i = 0; i < 20000 && !release.load(std::memory_order_acquire); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    expect(server.start(), "start() ok");

    int cfd = connectTo(cfg.port);
    expect(cfd >= 0, "client connected");
    expect(waitFor(5000, [&] { return reachedCb.load(std::memory_order_acquire); }),
           "the worker reached the open callback");

    const uint64_t before = server.stats().queueDrops.load();
    int accepted = 0;
    int refused  = 0;
    for (int i = 0; i < kFanOut; ++i) {
        const uint64_t id = connId.load(std::memory_order_acquire);
        const SendResult r = server.sendTo(id, "\x01", 1);
        if (r == SendResult::Ok) ++accepted;
        else                     ++refused;
    }

    release.store(true, std::memory_order_release);

    expect(accepted == kFanOut, "every frame of a full fan-out was queued");
    expect(server.stats().queueDrops.load() == before,
           "nothing was counted as a queue drop");
    std::printf("      queued %d of %d (%d refused), queueDrops=%llu\n",
                accepted, kFanOut, refused,
                (unsigned long long)(server.stats().queueDrops.load() - before));

    server.stop();
    ::close(cfd);

    std::printf("%s (%d failure%s)\n", gFailures == 0 ? "ALL PASS" : "FAILURES",
                gFailures, gFailures == 1 ? "" : "s");
    return gFailures == 0 ? 0 : 1;
}
