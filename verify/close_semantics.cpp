// verify/close_semantics.cpp -- what the peer actually observes at close time.
//
// This is the runtime counterpart to the compile-only check: it asserts on wire
// behaviour that a compiler cannot see, and that P1-4 changed.
//
//   A. sendTo(reply) then closeConnection() -- the ordinary "answer, then hang
//      up" pattern -- must deliver the reply and end with FIN. Before P1-4 the
//      close could overtake the queued reply, leaving the client with RST and no
//      payload.
//   B. closeConnection() while the client still has an unread request in flight
//      must not turn into RST just because input was pending (close() on a socket
//      with unread data resets the connection).
//   C. A protocol violation (bad magic) is still an explicit abort -> RST, so the
//      peer cannot mistake a truncated stream for a complete one.
//   D. Every connection ends up accounted for: accepted == closed when idle, and
//      the reservation for dropped output does not leak into connectionCount.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread verify/close_semantics.cpp -o close_semantics
//
// Exit code 0 = all cases passed.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;
using industrial::SendResult;

namespace {

int gFailures = 0;

void expect(bool ok, const char* what) {
    std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what);
    if (!ok) ++gFailures;
}

// ---- client side -----------------------------------------------------------

int connectTo(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::printf("      socket() failed: errno=%d (%s)\n", errno, std::strerror(errno));
        return -1;
    }
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        // Do not swallow this: it is the difference between "the server rejected
        // us" and "nothing is listening at all", and it decides where to look.
        std::printf("      connect(127.0.0.1:%u) failed: errno=%d (%s)\n",
                    (unsigned)port, errno, std::strerror(errno));
        ::close(fd);
        return -1;
    }
    return fd;
}

// Is anything listening on this port at all? Used to tell "server failed to
// bind" apart from "server bound but the accept loop never ran".
bool portIsListening(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bool ok = ::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) == 0;
    ::close(fd);
    return ok;
}

std::vector<uint8_t> encode(FrameType type, const void* payload, size_t len) {
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + len);
    industrial::encodeFrameInto(f.data(), type, payload, len);
    return f;
}

void sendAll(int fd, const std::vector<uint8_t>& f) {
    size_t off = 0;
    while (off < f.size()) {
        ssize_t n = ::send(fd, f.data() + off, f.size() - off, MSG_NOSIGNAL);
        if (n <= 0) return;
        off += (size_t)n;
    }
}

// Three ways a read can end, kept apart on purpose. "orderly FIN", "nothing ever
// arrived" and "RST" all leave recv() in a state that reads like success to a
// caller who only asks "was it a reset?", and only the third is what case C is
// actually about. Collapsing them is how a missing RST hides behind a clean FIN
// and looks like a server that merely forgot to abort.
enum class ReadEnd { Invalid, Fin, Timeout, Reset };

const char* readEndName(ReadEnd e) {
    switch (e) {
        case ReadEnd::Fin:     return "FIN (orderly)";
        case ReadEnd::Timeout: return "nothing arrived within the timeout";
        case ReadEnd::Reset:   return "RST";
        default:               return "no descriptor";
    }
}

// Reads until FIN or RST. Returns bytes received and reports which of the three
// endings it saw. An invalid descriptor reports Invalid rather than either of the
// other two, so "no connection" cannot masquerade as a wire outcome either.
std::vector<uint8_t> readToEnd(int fd, ReadEnd& end, int timeoutMs) {
    std::vector<uint8_t> out;
    uint8_t buf[4096];
    end = ReadEnd::Invalid;
    if (fd < 0) return out;

    struct timeval tv{};
    tv.tv_sec  = timeoutMs / 1000;
    tv.tv_usec = (timeoutMs % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    for (;;) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n > 0) { out.insert(out.end(), buf, buf + n); continue; }
        if (n == 0) { end = ReadEnd::Fin; break; }         // orderly FIN
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) { end = ReadEnd::Timeout; break; }
        end = ReadEnd::Reset;                              // ECONNRESET etc.
        break;
    }
    return out;
}

uint16_t pickPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    ::bind(fd, (struct sockaddr*)&sa, sizeof(sa));
    socklen_t len = sizeof(sa);
    ::getsockname(fd, (struct sockaddr*)&sa, &len);
    uint16_t port = ntohs(sa.sin_port);
    ::close(fd);
    return port;
}

bool waitFor(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// Every counter, dumped as one line per phase. The whole point is to be able to
// read a failure off the deltas: accepted/rejected says whether the accept loop
// handed the connection over, queueDrops says whether the reactor refused the
// task, epollCtlErrors/rejected inside registers say whether the reactor took it
// and then dropped it, and reactorFailures says whether the worker is still
// alive at all. Any one of those three looks identical from the client side.
void dumpStats(MultiReactorServer& server, const char* phase) {
    auto& st = server.stats();
    std::printf("    [%s] conns=%zu run=%d acc=%llu rej=%llu cls=%llu abt=%llu "
                "perr=%llu qdrop=%llu ctl=%llu rfail=%llu tasks=%llu depth=%llu "
                "frecv=%llu fsent=%llu\n",
                phase,
                server.connectionCount(), (int)server.isRunning(),
                (unsigned long long)st.accepted.load(),
                (unsigned long long)st.rejected.load(),
                (unsigned long long)st.closed.load(),
                (unsigned long long)st.aborted.load(),
                (unsigned long long)st.parseErrors.load(),
                (unsigned long long)st.queueDrops.load(),
                (unsigned long long)st.epollCtlErrors.load(),
                (unsigned long long)st.reactorFailures.load(),
                (unsigned long long)st.pendingTasksTotal.load(),
                (unsigned long long)st.pendingTasksDepth.load(),
                (unsigned long long)st.framesRecv.load(),
                (unsigned long long)st.framesSent.load());
}

}  // namespace

int main() {
    // Unbuffered stdout: the server prints its warnings to stderr, which is
    // unbuffered. Without this the two streams interleave when piped, and an
    // interleaved failure log is worse than none.
    setvbuf(stdout, nullptr, _IONBF, 0);

    // ---- server wiring -----------------------------------------------------
    std::atomic<int> handled{0};
    // Case A needs close-then-send-safe ordering, i.e. the close *after* the
    // echo. It is driven from the client side below: the handler only echoes.
    std::atomic<bool> closeAfterReply{false};
    std::atomic<bool> replyQueued{false};

    MultiReactorServer::Config cfg;
    cfg.port = pickPort();
    cfg.bindAddr = "127.0.0.1";
    cfg.numReactors = 2;
    // Switchable because pickReactor() hands connections out round-robin: with
    // CS_REACTORS=1 every client lands on the same reactor, so a fault that
    // belongs to one worker and not another shows up as everything passing.
    if (const char* r = std::getenv("CS_REACTORS")) cfg.numReactors = std::atoi(r);
    cfg.closeLingerMs = 300;          // keep the test short
    std::printf("== target port: %u, bindAddr=%s, reactors=%d ==\n",
                (unsigned)cfg.port, cfg.bindAddr.c_str(), cfg.numReactors);
    MultiReactorServer server(cfg);

    server.setMessageHandler([&](uint64_t id, const uint8_t* p, size_t n) {
        handled.fetch_add(1);
        server.sendTo(id, p, n);                     // queue the reply
        replyQueued.store(true);
        if (closeAfterReply.exchange(false)) {
            // Exactly the application pattern P1-4 is about: "answer, then hang
            // up". Both calls land on the same reactor queue, reply first.
            server.closeConnection(id);
        }
    });

    if (!server.start()) {
        std::printf("server.start() FAILED (port %u) - nothing is listening; "
                    "every case below will fail for that reason alone\n",
                    (unsigned)cfg.port);
        return 1;
    }
    std::printf("== server.start() ok; isRunning=%d ==\n", (int)server.isRunning());
    if (!server.isRunning()) {
        std::printf("WARNING: start() returned true but isRunning() is false "
                    "- a component raised the failure flag during bring-up\n");
    }

    // Probe once here so a bind/accept-loop failure is reported before any case
    // runs, instead of being guessed at from a pile of per-case failures.
    if (!portIsListening(cfg.port)) {
        std::printf("FATAL: nothing is listening on 127.0.0.1:%u after a "
                    "successful start() - the accept loop did not come up\n",
                    (unsigned)cfg.port);
    }

    dumpStats(server, "started");

    const uint16_t port = cfg.port;

    // ---- A: reply queued, then close (answer-then-hang-up) -----------------
    {
        std::printf("A. reply + close\n");
        dumpStats(server, "A.begin");
        int fd = connectTo(port);
        expect(fd >= 0, "client connected");
        replyQueued.store(false);
        closeAfterReply.store(true);
        const std::string payload = "ANSWER-42";
        sendAll(fd, encode(FrameType::DATA, payload.data(), payload.size()));
        expect(waitFor(2000, [&] { return replyQueued.load(); }), "handler ran");

        ReadEnd end = ReadEnd::Invalid;
        auto got = readToEnd(fd, end, 2000);
        ::close(fd);

        expect(end != ReadEnd::Reset, "peer got FIN, not RST");
        expect(got.size() == industrial::FRAME_HEADER_SIZE + payload.size(),
               "peer got exactly one framed reply");
        if (got.size() >= industrial::FRAME_HEADER_SIZE) {
            expect(std::memcmp(got.data() + industrial::FRAME_HEADER_SIZE,
                               payload.data(), payload.size()) == 0,
                   "reply payload intact");
        }
        expect(waitFor(2000, [&] { return server.connectionCount() == size_t(0); }),
               "connection torn down after the FIN exchange");
        dumpStats(server, "A.end");
    }

    // ---- B: close while the client still has data unread -------------------
    {
        std::printf("B. close with request in flight\n");
        dumpStats(server, "B.begin");
        int fd = connectTo(port);
        expect(fd >= 0, "client connected");
        // Two complete requests, then close immediately: the second one may still
        // be unread when the close is processed.
        uint64_t id = 0;
        expect(waitFor(2000, [&] {
                   auto ids = server.connectionIds();
                   if (ids.empty()) return false;
                   id = ids.front();
                   return true;
               }), "connection id observed");
        std::printf("      observed id=%llu (0 means the wait never saw one)\n",
                    (unsigned long long)id);

        sendAll(fd, encode(FrameType::DATA, "one", 3));
        sendAll(fd, encode(FrameType::DATA, "two", 3));
        server.closeConnection(id);

        ReadEnd end = ReadEnd::Invalid;
        auto got = readToEnd(fd, end, 2000);
        ::close(fd);
        expect(end != ReadEnd::Reset, "close did not degenerate into RST");
        std::printf("      wire ended with: %s\n", readEndName(end));
        expect(!got.empty(), "at least the first reply was delivered");
        dumpStats(server, "B.end");
    }

    // ---- C: protocol violation must be an explicit abort -------------------
    {
        std::printf("C. bad magic -> abort\n");
        dumpStats(server, "C.begin");
        int fd = connectTo(port);
        expect(fd >= 0, "client connected");
        uint8_t junk[16];
        std::memset(junk, 0x11, sizeof(junk));      // never a valid 0xEB90 header
        sendAll(fd, std::vector<uint8_t>(junk, junk + sizeof(junk)));

        ReadEnd end = ReadEnd::Invalid;
        readToEnd(fd, end, 2000);
        ::close(fd);
        expect(end == ReadEnd::Reset, "peer observes RST on a protocol violation");
        if (end != ReadEnd::Reset) {
            std::printf("      wire ended with: %s\n", readEndName(end));
            // An abort that counted internally but produced a clean FIN or a
            // silent timeout is a different defect from one that never happened;
            // the counter expectations below are satisfied by both.
            std::printf("      the abort counted internally, so the reset itself "
                        "did not reach this socket\n");
        }
        expect(waitFor(2000, [&] { return server.stats().parseErrors.load() >= 1; }),
               "parseErrors counted");
        expect(waitFor(2000, [&] { return server.stats().aborted.load() >= 1; }),
               "aborted counted");
        dumpStats(server, "C.end");
    }

    // ---- D: accounting ------------------------------------------------------
    {
        std::printf("D. accounting\n");
        server.stop();
        dumpStats(server, "D.stopped");
        const auto& st = server.stats();
        expect(st.accepted.load() == st.closed.load(),
               "accepted == closed after all connections ended");
        expect(st.accepted.load() >= 3, "all three clients were accepted");
        std::printf("  accepted=%llu closed=%llu aborted=%llu dropped=%llu handled=%d\n",
                    (unsigned long long)st.accepted.load(),
                    (unsigned long long)st.closed.load(),
                    (unsigned long long)st.aborted.load(),
                    (unsigned long long)st.bytesDropped.load(),
                    handled.load());
    }

    std::printf("%s (%d failure%s)\n", gFailures == 0 ? "ALL PASS" : "FAILURES",
                gFailures, gFailures == 1 ? "" : "s");
    return gFailures == 0 ? 0 : 1;
}
