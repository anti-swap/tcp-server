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
//   E. A peer that half-closes (request, then shutdown(SHUT_WR)) is still owed
//      the answer: every reply the handler queued must reach it, ahead of the
//      FIN. And while that answer drains, the reactor must not busy-poll --
//      level-triggered EPOLLIN on a FIN'd socket returns from every epoll_wait,
//      so eofReads (FIN wakes) must stay flat during the drain, not run away.
//   F. Twenty rounds of "close as soon as you know a request arrived". The
//      window this pokes at is microseconds wide: a request has been read and
//      handled, but the reply task its handler enqueued has not run yet. A
//      close landing in it finds nothing further to read, parks immediately,
//      and every reply task behind it then runs against a connection that is
//      already `closing` -- which submitSend refuses. Traced once under TSan:
//      SUBMIT_SEND(answer A) SUBMIT_CLOSE SUBMIT_SEND(answer B) TASK_SEND(A)
//      PARK_IMMEDIATE TASK_SEND(B) -> REFUSE, 11 of 22 bytes delivered.
//      One round mostly misses it; twenty mostly do not.
//   G. The two sent-counters must describe one exchange, not two. framesSent
//      used to move when a frame entered wbuf while bytesSent moved when bytes
//      left the socket, and nothing ever subtracted a frame a later abort
//      dropped: 255 frames "sent", 96 delivered, no way to reconcile. Both
//      now move at the kernel boundary, so with uniform frames framesSent
//      must equal bytesSent / wireSize -- exactly, whatever the socket buffer
//      happened to absorb.
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
// Answered once, up front: everything else in this file reads the wire, and how
// much of the wire there is to read depends on this.
bool gZeroLinger = true;

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

// Does this kernel turn SO_LINGER{on,0} into an RST the peer can observe?
//
// Case C depends on it, and it is a property of the machine rather than of the
// server: the same object code produces a reset on one kernel and an orderly FIN
// on another. Asking first keeps the suite comparable across machines instead of
// turning a platform limitation into a standing "known failure" that eventually
// stops being read.
bool kernelHonoursZeroLinger() {
    int ln = ::socket(AF_INET, SOCK_STREAM, 0);
    if (ln < 0) return false;
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(ln, (struct sockaddr*)&sa, sizeof(sa)) < 0) { ::close(ln); return false; }
    ::listen(ln, 4);

    socklen_t len = sizeof(sa);
    ::getsockname(ln, (struct sockaddr*)&sa, &len);
    int c = ::socket(AF_INET, SOCK_STREAM, 0);
    if (c < 0 || ::connect(c, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        if (c >= 0) ::close(c);
        ::close(ln);
        return false;
    }
    int sv = ::accept(ln, nullptr, nullptr);

    struct linger lg{};
    lg.l_onoff  = 1;
    lg.l_linger = 0;
    ::setsockopt(sv, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    ::close(sv);

    struct timeval tv{};
    tv.tv_sec = 1;
    ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    char b[8];
    ssize_t n = ::recv(c, b, sizeof(b), 0);
    const bool honouring = (n < 0 && errno == ECONNRESET);
    ::close(c);
    ::close(ln);
    return honouring;
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
                "frecv=%llu fsent=%llu eof=%llu\n",
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
                (unsigned long long)st.framesSent.load(),
                (unsigned long long)st.eofReads.load());
}

}  // namespace

int main() {
    // Unbuffered stdout: the server prints its warnings to stderr, which is
    // unbuffered. Without this the two streams interleave when piped, and an
    // interleaved failure log is worse than none.
    setvbuf(stdout, nullptr, _IONBF, 0);

    gZeroLinger = kernelHonoursZeroLinger();
    std::printf("== kernel turns SO_LINGER{on,0} into an observable RST: %s ==\n",
                gZeroLinger ? "yes" : "NO - case C cannot assert it");

    // ---- server wiring -----------------------------------------------------
    std::atomic<int> handled{0};
    // Case A needs close-then-send-safe ordering, i.e. the close *after* the
    // echo. It is driven from the client side below: the handler only echoes.
    std::atomic<bool> closeAfterReply{false};
    std::atomic<bool> replyQueued{false};
    // Case E: when >0, the handler answers with this many 32KB frames instead of
    // an echo -- an answer far larger than the client's receive buffer, so the
    // drain is still in progress while the client below is asleep.
    std::atomic<int> bigReplyFrames{0};

    MultiReactorServer::Config cfg;
    cfg.port = pickPort();
    cfg.bindAddr = "127.0.0.1";
    cfg.numReactors = 2;
    // Switchable because pickReactor() hands connections out round-robin: with
    // CS_REACTORS=1 every client lands on the same reactor, so a fault that
    // belongs to one worker and not another shows up as everything passing.
    if (const char* r = std::getenv("CS_REACTORS")) cfg.numReactors = std::atoi(r);
    cfg.closeLingerMs = 300;          // keep the test short
    // Case E needs an answer that cannot drain into the kernel while the client
    // sleeps: loopback autotuning can grow the socket buffers into the
    // megabytes, so both the per-connection wbuf and the pending ceiling are
    // raised to hold an 8MB answer. (backpressure.cpp does the same, for the
    // same reason.)
    cfg.wbufCapacity    = 32 * 1024 * 1024;
    cfg.maxPendingBytes = 32 * 1024 * 1024;
    std::printf("== target port: %u, bindAddr=%s, reactors=%d ==\n",
                (unsigned)cfg.port, cfg.bindAddr.c_str(), cfg.numReactors);
    MultiReactorServer server(cfg);

    server.setMessageHandler([&](uint64_t id, const uint8_t* p, size_t n) {
        handled.fetch_add(1);
        if (bigReplyFrames.load() > 0) {
            const std::vector<uint8_t> chunk(32 * 1024, 0x5A);
            for (int i = 0, total = bigReplyFrames.load(); i < total; ++i)
                server.sendTo(id, chunk.data(), chunk.size());
            replyQueued.store(true);
            return;
        }
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
        // Take the ids that already exist: everything below must talk about the
        // connection this block creates. Taking "whichever one is first" used to
        // hand back a stale connection left behind by an earlier case, and each
        // assertion then passed against the wrong socket while the one under test
        // was quietly never closed.
        const auto preexisting = server.connectionIds();
        int fd = connectTo(port);
        expect(fd >= 0, "client connected");
        uint64_t id = 0;
        expect(waitFor(2000, [&] {
                   for (uint64_t seen : server.connectionIds()) {
                       bool stale = false;
                       for (uint64_t old : preexisting) if (old == seen) stale = true;
                       if (!stale) { id = seen; return true; }
                   }
                   return false;
               }), "connection id observed");
        std::printf("      observed id=%llu (0 means the wait never saw one)\n",
                    (unsigned long long)id);

        // Two complete requests, then close. This case is about a request the
        // server has been handed but has not necessarily read yet, and whether
        // that stays true until the close lands cannot be left to timing: the
        // client would be asking the network stack to win a race against itself,
        // and sometimes it loses. bytesRecv says the bytes reached the server;
        // whether the reactor has read them is deliberately left open, which is
        // precisely what a graceful close has to cope with.
        const uint64_t sentAt = server.stats().bytesRecv.load();
        sendAll(fd, encode(FrameType::DATA, "one", 3));
        sendAll(fd, encode(FrameType::DATA, "two", 3));
        waitFor(2000, [&] {
            return server.stats().bytesRecv.load() >=
                   sentAt + 2 * (industrial::FRAME_HEADER_SIZE + 3);
        });
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
        if (gZeroLinger) {
            expect(end == ReadEnd::Reset, "peer observes RST on a protocol violation");
        } else {
            // Not assertable here, and not because the server declined to abort:
            // the two counters below say it did. A bare socket pair asked for the
            // same thing ends in an orderly FIN on this kernel, so there is no
            // close -- however forceful -- that reaches a peer as a reset.
            std::printf("  [SKIP] no wire RST is observable on this kernel: it "
                        "ignores SO_LINGER{on,0} (checked against a bare socket "
                        "pair), so case C can only assert the server's own "
                        "evidence here\n");
        }
        expect(waitFor(2000, [&] { return server.stats().parseErrors.load() >= 1; }),
               "parseErrors counted");
        expect(waitFor(2000, [&] { return server.stats().aborted.load() >= 1; }),
               "aborted counted");
        dumpStats(server, "C.end");
    }

    // ---- E: peer half-closes while its answer is still queued ---------------
    {
        std::printf("E. peer FIN with replies still queued\n");
        dumpStats(server, "E.begin");
        int fd = connectTo(port);
        expect(fd >= 0, "client connected");
        // Shrink the peer's receive buffer so the answer cannot drain into it
        // while the client sleeps: the server must park with bytes in wbuf,
        // which is exactly the state this case is about. (The kernel doubles
        // whatever is asked for here.)
        int rcvbuf = 16384;
        setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

        replyQueued.store(false);
        bigReplyFrames.store(250);     // 250 x 32KB = 8MB: cannot drain in one go
        const std::string req = "half-close-then-answer";
        sendAll(fd, encode(FrameType::DATA, req.data(), req.size()));
        // Done sending, still entitled to the answer. This is the HTTP/1.0-style
        // exchange: request, half-close, read until FIN.
        ::shutdown(fd, SHUT_WR);
        expect(waitFor(2000, [&] { return replyQueued.load(); }), "handler ran");
        bigReplyFrames.store(0);

        const size_t wantBytes =
            250 * (industrial::FRAME_HEADER_SIZE + 32 * 1024);

        // The parked-drain window: nobody reads at this end for 250ms, so wbuf
        // stays non-empty the whole time. eofReads counts how often the reactor
        // woke to the same FIN -- with read interest left armed that is once
        // per epoll_wait, i.e. thousands of times in this window.
        const uint64_t e0 = server.stats().eofReads.load();
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        const uint64_t e1 = server.stats().eofReads.load();
        std::printf("      eofReads +%llu over the 250ms parked window\n",
                    (unsigned long long)(e1 - e0));
        expect(e1 - e0 <= 20, "no busy-poll on the peer's FIN while draining");

        ReadEnd end = ReadEnd::Invalid;
        auto got = readToEnd(fd, end, 5000);
        ::close(fd);
        expect(end == ReadEnd::Fin, "wire ends with FIN, not RST or silence");
        std::printf("      wire ended with: %s, %zu of %zu bytes\n",
                    readEndName(end), got.size(), wantBytes);
        expect(got.size() == wantBytes,
               "every queued reply survived the peer's half-close");
        dumpStats(server, "E.end");
    }

    // ---- F: a close racing the reply it is racing ---------------------------
    //
    // Case B closes once and hopes. This one asks the same question twenty
    // times, because the window it is about is microseconds wide and a single
    // attempt mostly misses it: the two requests have been parsed and their
    // handler has run, but the reply tasks those handlers enqueued have not
    // been executed yet. When the close arrives in that window, drainInput
    // ThenClose finds nothing left to read, parks immediately, and the reply
    // tasks then run against a connection that is already `closing` -- which
    // submitSend refuses. From the client this looks like a server that took
    // the request in, ran the handler, delivered nothing, and hung up politely.
    {
        std::printf("F. 20 rounds of close racing the reply\n");
        dumpStats(server, "F.begin");
        const auto preexisting = server.connectionIds();
        const int rounds = 20;
        // Two requests of three bytes, answered by an echo: exactly these bytes
        // every round, or an answer was produced and then refused.
        const size_t wantRound = 2 * (industrial::FRAME_HEADER_SIZE + 3);
        int lost = 0;

        for (int round = 0; round < rounds; ++round) {
            int fd = connectTo(port);
            if (fd < 0) {
                std::printf("      round %d: connect failed\n", round);
                ++lost;
                continue;
            }
            sendAll(fd, encode(FrameType::DATA, "one", 3));
            sendAll(fd, encode(FrameType::DATA, "two", 3));
            // Close the instant the bytes are known to have arrived, which is
            // the earliest the application could know there is anything to wait
            // for -- polling every millimetre rather than every 10ms widens the
            // window from "almost never" to "most attempts".
            const uint64_t sentAt = server.stats().bytesRecv.load();
            for (int i = 0; i < 2000; ++i) {
                if (server.stats().bytesRecv.load() >=
                    sentAt + 2 * (industrial::FRAME_HEADER_SIZE + 3)) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            for (uint64_t id : server.connectionIds()) {
                bool stale = false;
                for (uint64_t old : preexisting) if (old == id) stale = true;
                if (!stale) server.closeConnection(id);
            }

            ReadEnd end = ReadEnd::Invalid;
            auto got = readToEnd(fd, end, 2000);
            ::close(fd);
            // Both requests were served before the close was asked for, so both
            // answers must arrive: anything less is an answer that existed and
            // was then refused. Judging a round on "did anything arrive at all"
            // lets a round that lost one of two replies pass.
            if (got.size() != wantRound) {
                ++lost;
                std::printf("      round %d: %zu of %zu bytes (%s)\n", round,
                            got.size(), wantRound, readEndName(end));
            }
        }
        std::printf("      %d of %d rounds lost a reply they had produced\n",
                    lost, rounds);
        expect(lost == 0, "no round loses an answer it had already produced");
        dumpStats(server, "F.end");
    }

    // ---- G: the sent-counters describe one exchange, not two ---------------
    //
    // A peer that never reads forces the close to park on a full wbuf, and the
    // linger deadline then aborts it with output undelivered -- exactly the
    // situation someone would consult framesSent to understand. Every reply
    // frame here is the same wire size, and at most one frame is ever mid-
    // flight, so if framesSent really counts frames that left the socket,
    // bytesSent / wireSize names the same number.
    {
        std::printf("G. counters across an aborted drain\n");
        MultiReactorServer::Config gcfg;
        gcfg.bindAddr = "127.0.0.1";
        gcfg.port = pickPort();
        gcfg.numReactors = 1;
        gcfg.idleTimeoutMs = 300;   // parks the close while output is stuck
        gcfg.closeLingerMs = 200;   // then expires it: abort with wbuf loaded
        gcfg.tickMs = 20;
        gcfg.wbufCapacity = 32 * 1024 * 1024;
        gcfg.maxPendingBytes = 32 * 1024 * 1024;

        MultiReactorServer gserver(gcfg);
        gserver.setMessageHandler([&](uint64_t id, const uint8_t*, size_t) {
            // ~33MB of answer against a peer that will not read a byte: the
            // socket buffer takes what it takes, everything else waits in
            // wbuf for the linger deadline.
            const std::string blob(industrial::MAX_FRAME_PAYLOAD, 'g');
            for (int i = 0; i < 512; ++i)
                if (gserver.sendTo(id, blob.data(), blob.size()) != SendResult::Ok) break;
        });
        expect(gserver.start(), "second server starts");
        int gfd = connectTo(gcfg.port);
        expect(gfd >= 0, "client connects");
        sendAll(gfd, encode(FrameType::DATA, "go", 2));
        // Read nothing on purpose: idle (300ms) parks the close, linger
        // (200ms) aborts it, the rest is margin for the tick.
        std::this_thread::sleep_for(std::chrono::milliseconds(1500));

        const auto& gst = gserver.stats();
        const uint64_t wire = industrial::FRAME_HEADER_SIZE + industrial::MAX_FRAME_PAYLOAD;
        const uint64_t fsent = gst.framesSent.load();
        const uint64_t bsent = gst.bytesSent.load();
        std::printf("      framesSent=%llu bytesSent=%llu bytesDropped=%llu aborted=%llu\n",
                    (unsigned long long)fsent, (unsigned long long)bsent,
                    (unsigned long long)gst.bytesDropped.load(),
                    (unsigned long long)gst.aborted.load());
        expect(gst.aborted.load() >= 1, "the linger deadline aborted the parked close");
        expect(gst.bytesDropped.load() > 0, "output was still undelivered at the abort");
        expect(bsent > 0, "some output did reach the socket");
        expect(fsent == bsent / wire,
               "framesSent counts frames that left, not frames that were queued");

        ::close(gfd);
        gserver.stop();
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
