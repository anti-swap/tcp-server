// industrial_tcp_server_mt.hpp
// 多线程 reactor + 全帧缓冲 + RAII + 显式关闭协议
// C++17 / Linux (epoll + eventfd)
#pragma once

#ifndef __linux__
#error "requires Linux (epoll + eventfd)"
#endif

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cassert>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <unordered_map>
#include <vector>
#include <array>
#include <shared_mutex>

namespace industrial {

// ==================== 帧协议 ====================
constexpr uint16_t FRAME_MAGIC       = 0xEB90;
constexpr size_t   FRAME_HEADER_SIZE = 8;
constexpr uint32_t MAX_FRAME_PAYLOAD = 64 * 1024;

enum class FrameType : uint16_t {
    DATA = 0x0001,
    PING = 0x0002,
    PONG = 0x0003,
};

inline int64_t nowMs() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// The header is eight bytes written big-endian, and from here on only this one
// place writes it: keeping two copies of the same byte orderings around is how
// they drift apart.
inline void writeFrameHeader(uint8_t* out, FrameType type, size_t len) noexcept {
    const uint16_t t = static_cast<uint16_t>(type);
    out[0] = (FRAME_MAGIC >> 8) & 0xFF;
    out[1] =  FRAME_MAGIC       & 0xFF;
    out[2] = (t >> 8) & 0xFF;
    out[3] =  t       & 0xFF;
    out[4] = (len >> 24) & 0xFF;
    out[5] = (len >> 16) & 0xFF;
    out[6] = (len >>  8) & 0xFF;
    out[7] =  len        & 0xFF;
}

inline const std::array<uint8_t, FRAME_HEADER_SIZE>& pongFrame() noexcept {
    static const std::array<uint8_t, FRAME_HEADER_SIZE> kPong = [] {
        std::array<uint8_t, FRAME_HEADER_SIZE> v{};
        writeFrameHeader(v.data(), FrameType::PONG, 0);
        return v;
    }();
    return kPong;
}

inline void encodeFrameInto(uint8_t* out, FrameType type,
                            const void* payload, size_t len) noexcept {
    // A const char* source is assumed. len <= MAX_FRAME_PAYLOAD is enforced by
    // sendTo, but this is an inline public entry point and nothing here would
    // complain about a caller who did not check: it would just write a length
    // the far side cannot honour.
    assert(len <= MAX_FRAME_PAYLOAD);
    writeFrameHeader(out, type, len);
    if (len > 0 && payload) std::memcpy(out + FRAME_HEADER_SIZE, payload, len);
}

// ==================== RAII fd ====================
struct FdGuard {
    int fd = -1;
    FdGuard() noexcept = default;
    explicit FdGuard(int f) noexcept : fd(f) {}
    FdGuard(FdGuard&& o) noexcept : fd(o.fd) { o.fd = -1; }
    FdGuard& operator=(FdGuard&& o) noexcept {
        if (this != &o) {
            reset(); fd = o.fd; o.fd = -1;
        }
        return *this;
    }
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;
    ~FdGuard() { reset(); }
    void reset(int f = -1) noexcept {
        if (fd >= 0) {
            ::close(fd);
        }
        fd = f;
    }
    int  release() noexcept { int f = fd; fd = -1; return f; }
    explicit operator bool() const noexcept { return fd >= 0; }
};

// ==================== 环形缓冲 ====================
class RingBuffer {
 public:
    RingBuffer() noexcept = default;
    explicit RingBuffer(size_t cap) { init(cap); }

    // 可能抛 bad_alloc：调用方决定策略（连接建立时一次分配）
    // A Conn is default-constructed and initialised a few lines later, so for a
    // short while cap_ really is 0. Every `+= N % cap_` below is UB for that
    // value -- integer division by zero, which is a trap rather than a wrong
    // answer -- so it is rejected here instead of being argued about per method.
    void init(size_t cap) {
        assert(empty());        // resizing over live data would drop it silently
        assert(cap > 0);
        buf_.resize(cap);
        cap_  = cap;
        head_ = 0;
        size_ = 0;
    }

    size_t capacity()  const noexcept { return cap_; }
    size_t readable()  const noexcept { return size_; }
    size_t writable()  const noexcept { return cap_ - size_; }
    bool   empty()     const noexcept { return size_ == 0; }
    bool   full()      const noexcept { return size_ == cap_; }

    // 写入，返回实际写入字节数（全帧写入需先检查 writable() >= n）
    size_t write(const uint8_t* p, size_t n) noexcept {
        if (cap_ == 0) return 0;   // uninitialised: see init()
        size_t w = std::min(n, writable());
        if (w == 0) return 0;
        size_t tail  = (head_ + size_) % cap_;
        size_t first = std::min(w, cap_ - tail);
        std::memcpy(buf_.data() + tail, p, first);
        if (w > first) std::memcpy(buf_.data(), p + first, w - first);
        size_ += w;
        return w;
    }

    size_t peek(uint8_t* p, size_t n) const noexcept {
        size_t r = std::min(n, size_);
        if (r == 0) return 0;
        size_t first = std::min(r, cap_ - head_);
        std::memcpy(p, buf_.data() + head_, first);
        if (r > first) std::memcpy(p + first, buf_.data(), r - first);
        return r;
    }

    // Same as peek, but starting `skip` bytes into the readable region. The
    // send path uses it to read a frame header that is not at the head.
    size_t peekAt(size_t skip, uint8_t* p, size_t n) const noexcept {
        if (skip >= size_) return 0;
        size_t r = std::min(n, size_ - skip);
        size_t at = (head_ + skip) % cap_;
        size_t first = std::min(r, cap_ - at);
        std::memcpy(p, buf_.data() + at, first);
        if (r > first) std::memcpy(p + first, buf_.data(), r - first);
        return r;
    }

    void consume(size_t n) noexcept {
        size_t r = std::min(n, size_);
        if (r == 0) return;    // also covers the uninitialised cap_ == 0 case
        head_ = (head_ + r) % cap_;
        size_ -= r;
    }

    struct Spans {
        const uint8_t* p1 = nullptr; size_t n1 = 0;
        const uint8_t* p2 = nullptr; size_t n2 = 0;
        int count = 0;
    };
    Spans spans() const noexcept {
        Spans s;
        if (size_ == 0) return s;
        size_t first = std::min(size_, cap_ - head_);
        s.p1 = buf_.data() + head_;
        s.n1 = first;
        if (size_ > first) {
            s.p2 = buf_.data(); s.n2 = size_ - first; s.count = 2;
        }
        else s.count = 1;
        return s;
    }

    void clear() noexcept { head_ = 0; size_ = 0; }

 private:
    std::vector<uint8_t> buf_;
    size_t cap_  = 0;
    size_t head_ = 0;
    size_t size_ = 0;
};

// ==================== 统计 ====================
struct ServerStats {
    std::atomic<uint64_t> accepted{0};
    std::atomic<uint64_t> rejected{0};
    std::atomic<uint64_t> closed{0};
    std::atomic<uint64_t> framesRecv{0};
    // 帧的最后一个字节到达内核时才计（drainWbufToSocket），与 bytesSent 同一
    // 计量点。若在"进 wbuf"时计，abort 丢掉的帧会永久留在计数里且无处扣减
    // （实测一次 abort：计了 511 帧、实际送达 48 帧）。
    std::atomic<uint64_t> framesSent{0};
    std::atomic<uint64_t> bytesRecv{0};
    std::atomic<uint64_t> bytesSent{0};
    std::atomic<uint64_t> queueDrops{0};       // 全帧入队被拒次数
    std::atomic<uint64_t> bytesDropped{0};     // 连接关闭时被丢弃的待发字节
    std::atomic<uint64_t> aborted{0};          // 强制断开（RST）次数，区别于 shutdown 后正常关闭
    std::atomic<uint64_t> framesDroppedOnClose{0};  // 已在关闭流程中、因 FIN 已发而丢弃的待发帧
    std::atomic<uint64_t> parseErrors{0};
    std::atomic<uint64_t> idleTimeouts{0};
    std::atomic<uint64_t> epollCtlErrors{0};
    std::atomic<uint64_t> reactorFailures{0};  // reactor 线程崩溃/退出异常
    // accept4 因耗尽文件描述符而失败的次数。这是唯一能在启动时预测到的 accept
    // 失败，因此也必须可数：没有它，唯一的症状是"它不再接受连接了，且一语不发"。
    std::atomic<uint64_t> acceptErrors{0};
    std::atomic<uint64_t> pendingTasksTotal{0};  // 累计投递
    std::atomic<uint64_t> pendingTasksDepth{0};  // 当前队列深度（gauge）
    // recv 返回 0（对端 FIN）的次数。一条对端已半关闭的连接在 level-triggered
    // 下会让 epoll_wait 每次都立即返回同一个 EPOLLIN，所以这个数字同时是
    // "reactor 是否在对着同一个 FIN 空转"的探针：停靠排空期间它应几乎不动。
    std::atomic<uint64_t> eofReads{0};
    // closeConnection() 发出去的 Submission 被队列拒绝的次数。这些关闭请求既
    // 没有落到连接上，也没有任何计数器记账，唯一的下场是等 idle 超时。
    std::atomic<uint64_t> closeRequestsDropped{0};
    // sendmsg 返回 EPIPE 的次数：某个 socket 的写方向已经判死。至今每种能到达它的
    // 路径都被别处的守卫先拦下了（本地 shutdown 之后 handleWrite 与 parkClose 的
    // finSent 检查会先返回，对端 RST 则在 loop() 里先走 EPOLLERR/EPOLLHUP 分支），
    // 所以它预期恒为 0——这个计数器的意义正是让"不可达"是可以被读出来的，而不是
    // 永远只是一个判断。一旦它动了，说明上面的推理失效了，该回头复查这条路径。
    std::atomic<uint64_t> epipeWrites{0};
};

// ==================== 发送结果 ====================
enum class SendResult : int {
    Ok = 0,
    NotConnected,   // 连接不存在
    Closed,         // 连接已关闭 / 服务停止
    TooLarge,       // 超过 MAX_FRAME_PAYLOAD
    Overloaded,     // 逻辑上限或队列满
    InvalidArgument,
};

class MultiReactorServer;
class SubReactor;

// One reservation follows a socket from the accept queue through connection
// lifetime. Every failure/discard path releases it through the same destructor.
// The RMWs are relaxed, same policy as every other gauge: RMW cannot lose an
// update, nothing non-atomic is published through the count, and the accept
// loop's cap check is advisory by nature (the load races the reservation by
// definition). The ++/-- operator forms would quietly mean seq_cst.
struct Admission {
    std::atomic<size_t>& count;
    explicit Admission(std::atomic<size_t>& value) noexcept : count(value) {
        count.fetch_add(1, std::memory_order_relaxed);
    }
    Admission(const Admission&) = delete;
    Admission& operator=(const Admission&) = delete;
    Admission(Admission&&) = delete;
    Admission& operator=(Admission&&) = delete;
    ~Admission() { count.fetch_sub(1, std::memory_order_relaxed); }
};

// ==================== 连接 ====================
struct Conn {
    uint64_t    id    = 0;
    int         fd    = -1;
    std::string peer;
    SubReactor* owner = nullptr;

    // reactor 线程独占
    int64_t    lastRecvMs = 0;
    RingBuffer rbuf;
    RingBuffer wbuf;
    // Target state of this socket's EPOLLOUT bit, owned by the worker thread.
    // armWrite/disarmWrite are idempotent transitions toward it, never blind
    // epoll_ctl calls, so a stale EPOLLOUT event cannot tear down a live write
    // interest (and a queued frame cannot be stranded without a wakeup).
    bool       writeArmed = false;
    // Bytes of the frame at the head of wbuf that have not reached the kernel.
    // wbuf only ever receives whole frames, so the head is either inside
    // exactly one frame or exactly at a frame boundary; this field says which,
    // so the drain can count a frame when its last byte leaves instead of when
    // it is queued. Reactor-thread-only, like wbuf itself.
    size_t      sendFrameRemain = 0;
    // -------- close protocol --------
    // closing: a graceful close has been requested and is being honoured (output
    //          drained, FIN sent or imminent). Submissions are refused from here
    //          on, but the peer keeps reading what we owe it.
    // finSent: FIN has actually left. Closing before that would drop queued bytes
    //          and turn the polite close into the RST this whole mechanism avoids.
    // The two are separate on purpose: "closed" must not mean "socket is gone",
    // or an application that sends a reply and then closes would never get the
    // reply out. Reads keep running throughout.
    bool       closing = false;
    bool       finSent = false;
    // A graceful close whose final read dispatched requests: their replies are
    // still queued, and the FIN waits for them. Bounds the wait to one round --
    // a peer that keeps sending cannot hold the close open with more input.
    // finishPeerClosed reuses the same once-guard for the mirror image: a peer
    // that half-closed while its answers were still queued as tasks.
    bool       waitingOnReplies = false;
    // Peer's FIN has been seen (recv returned 0). Set once, in handleRead;
    // handleWrite consults it to finish a parked close deterministically the
    // moment the drain completes, instead of waiting for a spurious EPOLLIN.
    bool       peerClosed = false;
    int64_t    closeStartedAtMs = 0;   // when the graceful close was parked
    int64_t    finSentAtMs = 0;        // when shutdown(SHUT_WR) returned

    // 跨线程
    std::atomic<bool>   isClosed{false};
    // Admission credit for output: bytes reserved and not yet handed to the
    // kernel, whether they are still sitting in a queued send task or already
    // sitting in wbuf. It is one number covering both because neither one alone
    // says whether the peer is being kept up with.
    //
    // Lifecycle, which has to be read as the three places it is touched rather
    // than the one place it happens to stay level:
    //   1. reserve    -- enqueueing a frame adds its length (PONG included);
    //   2. migrate    -- moving a frame from a queued task into wbuf changes
    //                    nothing, and that is the only reason some paths nearby
    //                    do not visibly adjust it;
    //   3. release    -- subtracting what was actually written, what was dropped
    //                    whole (capacity reject, closing connection, refused
    //                    PONG), and what was still unsent when we tore down.
    //
    // Every release runs on the connection's owning reactor, so release/release
    // never races. Reserving races between application threads and that same
    // reactor's PONG path, and relaxed is enough for all of it: nothing
    // non-atomic is published through this counter, the frame itself crosses
    // under pendingMutex_, and RMW already guarantees no update is lost -- which
    // is the property that matters, because a lost decrement is a permanent
    // leak and a visible one in any watermark built on top. Ordering past that
    // would only buy freshness, and freshness is not something this number can
    // promise anyway: it is a best-effort ceiling, so reading a stale value
    // costs one refused frame, never correctness.
    std::atomic<size_t> pendingBytes{0};
    bool readPending = false;  // Worker-owned continuation after a parsing budget.
    Conn* readNext = nullptr;
    Conn* readPrev = nullptr;
    std::shared_ptr<Admission> admission;

    ~Conn() noexcept {
        // 兜底：只 close 一次。正常路径 closeConn 已置 fd=-1
        if (fd >= 0) {
            ::close(fd); fd = -1;
        }
    }
};

// How far a drain got. These used to be 0 / 1 / -1 with two different outcomes
// sharing the 0: the while-loop ending because wbuf was empty, and EPIPE. The
// first means every queued byte reached the kernel and the caller should go on
// to whatever comes next; the second means not one more byte will ever leave,
// and treating them alike is what put the connection's fate in the hands of
// `wbuf.empty()` at each call site -- a fact none of them said out loud.
enum class DrainOutcome {
    Drained,   // wbuf is empty: everything owed has reached the kernel
    Stalled,   // fair-share budget spent, or EAGAIN: EPOLLOUT will resume it
    Dead,      // EPIPE: this socket's write direction is finished, permanently
    Error,     // any other failure: fatal
};

// ==================== SubReactor ====================
class SubReactor {
 public:
    SubReactor(int idx, MultiReactorServer* server) noexcept
        : idx_(idx), server_(server) {}
    ~SubReactor() noexcept { stop(); }

    SubReactor(const SubReactor&) = delete;
    SubReactor& operator=(const SubReactor&) = delete;

    bool start() noexcept;
    void stop()  noexcept;

    // Submit through the server's lifetime barrier. New-connection submission
    // always consumes its FdGuard; failed sends require a byte-reservation rollback.
    bool submitNewConnection(FdGuard fd, std::string peer, uint64_t id,
                             std::shared_ptr<Admission> admission) noexcept;
    bool submitSend(uint64_t id, std::vector<uint8_t> frame) noexcept;
    bool submitClose(uint64_t id) noexcept;

    int idx() const noexcept { return idx_; }

 private:
    void loop() noexcept;
    void wakeup() noexcept;
    void drainWakeup() noexcept;
    void processPending() noexcept;
    // Returns the number of frames this call handed to the message handler. Zero
    // means the read produced no work at all, which drainInputThenClose() uses to
    // tell "the peer asked for nothing more" from "answers are queued behind us".
    int  handleRead(Conn* c) noexcept;
    void handleWrite(Conn* c) noexcept;
    void parseFrames(Conn* c, int& framesLeft) noexcept;
    // graceful=true: whatever we still owe the peer is delivered before the FIN,
    //                and the socket is closed only after the peer's own FIN (or
    //                closeLingerMs). Use it whenever the peer is a legitimate
    //                counterparty that should not lose its last reply.
    // graceful=false: abort now -- pending output is dropped and an RST is
    //                requested. For protocol violations, resource exhaustion and
    //                fault paths, where delivering a half-processed stream would
    //                be worse than the reset.
    // Default graceful=true: see the definition for what the two modes mean.
    void closeConn(Conn* c, const char* reason, bool graceful = true) noexcept;
    void teardownClosed(Conn* c, const char* reason, bool graceful) noexcept;
    void parkClose(Conn* c, const char* reason) noexcept;
    bool tryBeginWriteShutdown(Conn* c) noexcept;
    // A graceful close asked for while the peer's request is still unread: reads
    // and answers it before the FIN goes out. See the definition for why the FIN
    // has to wait a queue round, and why it waits at most one.
    void drainInputThenClose(Conn* c) noexcept;
    // Precondition: parseFrames has already drained every complete frame, so any
    // reply it triggered is queued ahead of the FIN this may send. handleRead
    // establishes that before calling in; it is not checked here.
    void finishPeerClosed(Conn* c) noexcept;
    void unregisterConn(Conn* c, bool graceful = true) noexcept;
    void discardPendingOutput(Conn* c) noexcept;
    void finishClose(std::shared_ptr<Conn>& c) noexcept;
    void flushCloseCallbacks() noexcept;
    void armWrite(Conn* c) noexcept;
    void disarmWrite(Conn* c) noexcept;
    void doTick() noexcept;
    DrainOutcome drainWbufToSocket(Conn* c) noexcept;
    bool enqueue(std::function<void()> task) noexcept;
    void cleanup() noexcept;
    void scheduleRead(Conn* c) noexcept;
    void unscheduleRead(Conn* c) noexcept;
    Conn* readHead_ = nullptr;
    Conn* readTail_ = nullptr;

    int                 idx_;
    MultiReactorServer* server_;

    int               epollFd_ = -1;
    int               wakeFd_  = -1;
    // Worker-owned: false means this reactor accepts no more submissions.
    std::atomic<bool> running_{false};
    std::thread       thread_;

    // Queue cap, and it is **per reactor** -- which is a different number from
    // "per server" in a way that matters. pickReactor() spreads submissions out,
    // so a broadcast to 8k connections across 4 reactors asks each queue for 2k
    // slots, not 8k; scaling this by the server's total connection count would
    // be wrong in the other direction.
    //
    // What it does have to track is how many connections *this* reactor can be
    // asked to serve, because that is what a broadcast costs. A flat 8192 looked
    // generous until the box grew: past roughly that many connections one
    // broadcast fills whichever queue it lands on, and everything queued after
    // it is counted as queueDrops -- which reads as "the peers are not
    // draining" rather than "a constant was picked before anyone knew the
    // fan-out". Four frames of headroom per expected connection is enough for a
    // reply plus a close per broadcast, and the floor keeps small servers where
    // they were.
    static constexpr size_t kMinPendingTasks = 8192;
    size_t pendingTaskCap_ = kMinPendingTasks;
    std::mutex                        pendingMutex_;
    std::vector<std::function<void()>> pendingTasks_;
    std::vector<std::function<void()>> activeTasks_;

    // Deferred close notifications. Kept parallel and cleared together; entries
    // are only touched by the worker thread.
    std::vector<std::shared_ptr<Conn>> pendingCloses_;
    std::vector<uint64_t>              pendingCloseIds_;
    std::vector<std::string>           pendingClosePeers_;
    std::vector<std::string>           pendingCloseReasons_;

    std::unordered_map<int, Conn*>                      fdIndex_;
    std::unordered_map<uint64_t, std::shared_ptr<Conn>> conns_;

    std::vector<uint8_t> frameScratch_;
};

// ==================== MultiReactorServer ====================
class MultiReactorServer {
 public:
    // Install handlers before start; never replace them while running. Handlers
    // run concurrently across workers and must not throw or destroy this server.
    // Message payload/peer/reason references are borrowed for the call only.
    // sendTo/closeConnection are permitted. stop from a callback requests shutdown
    // only; the controlling thread must subsequently call stop to join threads.
    // Close/state-false callbacks are deferred: they run on the worker thread
    // after the closing connection's stack frame has unwound, so they may drop
    // the last external owner of this server.
    using MessageHandler   = std::function<void(uint64_t, const uint8_t*, size_t)>;
    using ConnStateHandler = std::function<void(uint64_t, const std::string&, bool)>;
    using CloseHandler = std::function<void(uint64_t, const std::string&, const char*)>;

    struct Config {
        std::string bindAddr        = "0.0.0.0";
        uint16_t    port            = 0;
        // Must be > 0 (start() refuses otherwise): Linux clamps listen()'s
        // backlog only from above, so 0 would pass straight through as a
        // zero-length accept queue -- connections refused under load, silently.
        int         backlog         = 1024;
        int         numReactors     = 0;
        int         maxConnections  = 10000;
        int         idleTimeoutMs   = 120000;
        int         tickMs          = 500;
        size_t      rbufCapacity    = FRAME_HEADER_SIZE + MAX_FRAME_PAYLOAD;
        size_t      wbufCapacity    = 256 * 1024;   // 256KB
        size_t      maxPendingBytes = 256 * 1024;   // <= wbufCapacity
        size_t      writeBudgetBytes = 256 * 1024;
        // Grace period for a half-closed connection: after our FIN went out we
        // keep reading until the peer's FIN, but never longer than this. Bounds
        // the descriptor cost of a peer that ACKs the FIN and never answers it.
        // Must be > 0 (start() refuses otherwise): with no deadline a parked
        // close leaks the fd and its admission slot forever.
        int         closeLingerMs    = 5000;

        // Ceiling on reactors. Beyond a handful past the core count the only
        // thing extra workers buy is more cross-thread hand-off.
        static constexpr int kMaxReactors = 64;

        // What this configuration actually resolves to, with "0 means decide for
        // me" resolved and the ceiling applied. Two places computing it
        // independently is how a number like this ends up disagreeing with
        // itself, which is what was happening: start() clamped to a literal 64
        // while everything that needed to know the count had to guess again.
        int resolvedReactorCount() const noexcept {
            int n = numReactors > 0
                        ? numReactors
                        : static_cast<int>(std::thread::hardware_concurrency());
            if (n <= 0) n = 1;
            if (n > kMaxReactors) n = kMaxReactors;
            return n;
        }
    };

    explicit MultiReactorServer(Config cfg) noexcept : cfg_(std::move(cfg)) {}
    ~MultiReactorServer() noexcept { stop(); }

    MultiReactorServer(const MultiReactorServer&) = delete;
    MultiReactorServer& operator=(const MultiReactorServer&) = delete;

    void setMessageHandler(MessageHandler h)     noexcept { msgHandler_   = std::move(h); }
    void setConnStateHandler(ConnStateHandler h) noexcept { stateHandler_ = std::move(h); }
    // reason is borrowed and valid for the callback; includes shutdown closures.
    void setCloseHandler(CloseHandler h) noexcept { closeHandler_ = std::move(h); }

    bool start() noexcept;
    void stop()  noexcept;
    void requestStop() noexcept { requestStopInternal(); }
#ifdef INDUSTRIAL_TCP_TESTING
    std::atomic<bool> failWorkerForTest{false};
    std::atomic<bool> failAcceptForTest{false};
#endif
    void wait()  noexcept;

    // Ok means queued, not delivered. Shutdown or disconnection can discard work.
    SendResult sendTo(uint64_t connId, const void* data, size_t len) noexcept;
    // True means the request reached the owning reactor's queue. False means it
    // did not -- the reactor refused it (its queue is at the cap) or there is no
    // owner to hand it to -- and the connection is still open. Either way it is
    // counted into closeRequestsDropped: a close nobody asked for is bad, but a
    // close nobody was told about is how a connection outlives its own owner.
    bool       closeConnection(uint64_t connId) noexcept;

    size_t connectionCount() const noexcept;
    std::vector<uint64_t> connectionIds() const;

    const ServerStats& stats() const noexcept { return stats_; }

    // SubReactor 内部接口
    bool   onConnectionOpened(uint64_t id, std::shared_ptr<Conn> c) noexcept;
    void   onConnectionClosed(uint64_t id) noexcept;
    // Running means: lifecycle reached Running AND no component has failed.
    // A reactor/accept thread failure only raises failed_; it never rewrites the
    // lifecycle state, so start()'s success/failure verdict stays unambiguous.
    bool   isRunning() const noexcept {
        return life_.load(std::memory_order_acquire) == Life::Running &&
               !failed_.load(std::memory_order_acquire);
    }
    int    tickMs() const noexcept { return cfg_.tickMs; }
    int    idleTimeoutMs() const noexcept { return cfg_.idleTimeoutMs; }
    int    closeLingerMs() const noexcept { return cfg_.closeLingerMs; }
    size_t rbufCapacity() const noexcept { return cfg_.rbufCapacity; }
    size_t wbufCapacity() const noexcept { return cfg_.wbufCapacity; }
    ServerStats&      statsRef()          noexcept { return stats_; }
    MessageHandler&   msgHandlerRef()     noexcept { return msgHandler_; }
    ConnStateHandler& stateHandlerRef()   noexcept { return stateHandler_; }

 private:
    friend class SubReactor;

    // Lifecycle states. Only the thread holding lifecycleMutex_ moves the state
    // out of/into Stopped; threads that merely observe a fatal error use fail().
    enum class Life : int { Stopped = 0, Starting = 1, Running = 2, Stopping = 3 };

    // Single upward-only failure flag shared by the accept loop and all reactors.
    // Independent of the lifecycle state so it can be raised concurrently.
    void fail() noexcept { failed_.store(true, std::memory_order_release); }
    // Step down from Running and wake everyone; no-op unless Running.
    void  requestStopInternal() noexcept {
        Life expected = Life::Running;
        if (life_.compare_exchange_strong(expected, Life::Stopping,
                                          std::memory_order_acq_rel)) {
            wakeAcceptLoop();
        }
    }
    void wakeAcceptLoop() noexcept;

    bool        initListenSocket() noexcept;
    void        acceptLoop(std::promise<bool>& ready) noexcept;
    SubReactor* pickReactor() noexcept;

    Config cfg_;
    int listenFd_ = -1;
    int epollFd_  = -1;
    int wakeFd_   = -1;

    std::atomic<Life>     life_{Life::Stopped};
    std::atomic<bool>     failed_{false};
    std::atomic<uint64_t> nextConnId_{1};
    std::atomic<size_t>   nextReactor_{0};
    // Lifecycle calls are serialized; submissions hold their barrier only until
    // queued. stop releases it before joining so worker callbacks can finish.
    std::mutex lifecycleMutex_;
    std::mutex acceptFdMutex_;  // Covers publication, wake writes, and fd teardown.
    inline static thread_local MultiReactorServer* callbackServer_ = nullptr;
    struct CallbackScope {
        MultiReactorServer* previous;
        explicit CallbackScope(MultiReactorServer* server) noexcept
            : previous(callbackServer_) { callbackServer_ = server; }
        ~CallbackScope() { callbackServer_ = previous; }
    };
    std::shared_mutex submissionMutex_;
    // Queued accepted sockets + live connections, owned by Admission tickets.
    std::atomic<size_t> admitted_{0};

    std::vector<std::unique_ptr<SubReactor>> reactors_;
    std::thread                              acceptThread_;

    mutable std::shared_mutex                           connsMutex_;
    std::unordered_map<uint64_t, std::shared_ptr<Conn>> conns_;

    ServerStats      stats_;
    MessageHandler   msgHandler_;
    ConnStateHandler stateHandler_;
    CloseHandler     closeHandler_;
};

// ====================================================================
//                         SubReactor 实现
// ====================================================================

inline bool SubReactor::start() noexcept {
    if (running_.exchange(true)) return true;

    epollFd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epollFd_ < 0) {
        running_.store(false); return false;
    }

    wakeFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeFd_ < 0) {
        ::close(epollFd_); epollFd_ = -1;
        running_.store(false); return false;
    }

    struct epoll_event ev{};
    ev.events = EPOLLIN; ev.data.fd = wakeFd_;
    if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, wakeFd_, &ev) < 0) {
        ::close(wakeFd_); ::close(epollFd_);
        wakeFd_ = epollFd_ = -1; running_.store(false); return false;
    }

    // Computed here rather than in the constructor: picking reactor count and
    // connection ceiling are both decisions the server makes later, and this is
    // the first point at which both are final.
    const size_t share = static_cast<size_t>(server_->cfg_.maxConnections) /
                         static_cast<size_t>(server_->cfg_.resolvedReactorCount());
    pendingTaskCap_ = std::max(kMinPendingTasks, share * 4);
    try {
        pendingTasks_.reserve(pendingTaskCap_);
        activeTasks_.reserve(pendingTaskCap_);
        frameScratch_.resize(MAX_FRAME_PAYLOAD);
    } catch (...) {
        ::close(wakeFd_); ::close(epollFd_);
        wakeFd_ = epollFd_ = -1; running_.store(false); return false;
    }

    try {
        thread_ = std::thread([this]() noexcept { loop(); });
    } catch (...) {
        ::close(wakeFd_); ::close(epollFd_);
        wakeFd_ = epollFd_ = -1; running_.store(false); return false;
    }
    return true;
}

inline void SubReactor::stop() noexcept {
    if (server_->callbackServer_ == server_) {
        server_->requestStop(); return;
    }
    {
        std::lock_guard<std::mutex> lk(pendingMutex_);
        running_.store(false);
        wakeup();
    }
    if (thread_.joinable()) thread_.join();
    cleanup();
    std::lock_guard<std::mutex> lk(pendingMutex_);
    if (wakeFd_  >= 0) {
        ::close(wakeFd_);  wakeFd_  = -1;
    }
    if (epollFd_ >= 0) {
        ::close(epollFd_); epollFd_ = -1;
    }
}

inline void SubReactor::cleanup() noexcept {
    MultiReactorServer::CallbackScope callbacks(server_);
    // Discard waiting tasks; shared FdGuard captures close unclaimed sockets.
    {
        std::lock_guard<std::mutex> lk(pendingMutex_);
        server_->statsRef().pendingTasksDepth.fetch_sub(pendingTasks_.size(),
                                                        std::memory_order_relaxed);
        pendingTasks_.clear();
    }

    while (!conns_.empty()) {
        // Shutdown cannot wait out anyone's half-close -- the reactor is leaving
        // and there is no thread left to deliver a parked connection's last
        // bytes. Abort explicitly, and account the undelivered bytes as drops
        // rather than pretending they went out.
        std::shared_ptr<Conn> c = conns_.begin()->second;
        Conn* raw = c.get();
        c.reset();
        closeConn(raw, "server shutdown", false);
    }
    flushCloseCallbacks();
    activeTasks_.clear();
    conns_.clear();
    fdIndex_.clear();
}

inline void SubReactor::wakeup() noexcept {
    if (wakeFd_ < 0) return;
    uint64_t v = 1;
    ssize_t r = ::write(wakeFd_, &v, sizeof(v));
    (void)r;
}

// eventfd 一次 read 即清零累积计数，无需循环
inline void SubReactor::drainWakeup() noexcept {
    uint64_t v = 0;
    ssize_t r = ::read(wakeFd_, &v, sizeof(v));
    (void)r;
}

inline void SubReactor::processPending() noexcept {
    {
        std::lock_guard<std::mutex> lk(pendingMutex_);
        if (pendingTasks_.empty()) return;
        activeTasks_.swap(pendingTasks_);
        server_->statsRef().pendingTasksDepth.fetch_sub(activeTasks_.size(),
                                                        std::memory_order_relaxed);
    }
    for (auto& t : activeTasks_) {
        if (!running_.load() || !server_->isRunning()) break;
        t();
        flushCloseCallbacks();
    }
    activeTasks_.clear();
    flushCloseCallbacks();
}

inline bool SubReactor::enqueue(std::function<void()> task) noexcept {
    {
        std::lock_guard<std::mutex> lk(pendingMutex_);
        if (!running_.load() || !server_->isRunning() || pendingTasks_.size() >= pendingTaskCap_) {
            server_->statsRef().queueDrops.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        // Both batch vectors reserve this limit before the worker starts.
        pendingTasks_.emplace_back(std::move(task));
        server_->statsRef().pendingTasksTotal.fetch_add(1, std::memory_order_relaxed);
        server_->statsRef().pendingTasksDepth.fetch_add(1, std::memory_order_relaxed);
        // Serialize notification with stop's descriptor cleanup. No producer
        // touches wakeFd_ after releasing this lock.
        wakeup();
    }
    return true;
}

inline bool SubReactor::submitNewConnection(
        FdGuard fd, std::string peer, uint64_t id,
        std::shared_ptr<Admission> admission) noexcept {
    std::function<void()> task;
    try {
        auto socket = std::make_shared<FdGuard>(std::move(fd));
        task = [this, socket, admission = std::move(admission),
                peer = std::move(peer), id]() mutable noexcept {
            try {
                auto c = std::make_shared<Conn>();
                c->id = id;
                c->peer = std::move(peer);
                c->owner = this;
                c->admission = std::move(admission);
                c->lastRecvMs = nowMs();
                c->rbuf.init(server_->rbufCapacity());
                c->wbuf.init(server_->wbufCapacity());

                struct epoll_event ev{};
                ev.events = EPOLLIN; ev.data.fd = socket->fd;
                if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, socket->fd, &ev) < 0) {
                    server_->statsRef().epollCtlErrors.fetch_add(1, std::memory_order_relaxed);
                    server_->statsRef().rejected.fetch_add(1, std::memory_order_relaxed);
                    return;  // The task's socket guard closes the descriptor.
                }
                c->fd = socket->release();

                try {
                    fdIndex_[c->fd] = c.get();
                    conns_[id] = c;
                } catch (...) {
                    // Roll back through the same path every other failure uses, so
                    // the epoll registration does not outlive the map entry and get
                    // silently re-attributed when the fd number is recycled.
                    unregisterConn(c.get());
                    server_->statsRef().rejected.fetch_add(1, std::memory_order_relaxed);
                    return;
                }
                if (!server_->onConnectionOpened(id, c)) {
                    unregisterConn(c.get());
                    server_->statsRef().rejected.fetch_add(1, std::memory_order_relaxed);
                    return;
                }
                // Counted only once the socket is fully owned by both maps: an
                // accepted client must not inflate accepted when the reactor then
                // rejects it, otherwise accepted - closed stops matching reality.
                server_->statsRef().accepted.fetch_add(1, std::memory_order_relaxed);

                auto& sh = server_->stateHandlerRef();
                if (sh) sh(id, c->peer, true);
            } catch (...) {
                    // Before registration, the socket guard closes on failure.
                    server_->statsRef().rejected.fetch_add(1, std::memory_order_relaxed);
                }
            };   // NOTE: accepted is bumped inside, not by the accept loop
    } catch (...) {
        // Local guards retain ownership if constructing the task fails.
        server_->statsRef().queueDrops.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    return enqueue(std::move(task));
}

inline bool SubReactor::submitSend(uint64_t id,
                                   std::vector<uint8_t> frame) noexcept {
    std::function<void()> task;
    try {
        task = [this, id, f = std::move(frame)]() noexcept {
            auto it = conns_.find(id);
            if (it == conns_.end()) return;
            Conn* c = it->second.get();
            // A parked close still accepts the reply that was queued before it:
            // submitClose only marks the connection and then returns to this batch,
            // so this task runs first. Anything arriving after that point is
            // refused here -- the FIN is already reserved for what we owe the peer.
            if (c->isClosed.load(std::memory_order_acquire) || c->closing) {
                c->pendingBytes.fetch_sub(f.size(), std::memory_order_relaxed);
                // One refusal, two questions, two counters: how many replies
                // never went out (queueDrops) and how much data the peer never
                // received (bytesDropped). Recording whichever one this branch
                // happened to reach made the two incomparable across a close --
                // the same event would show up as a frame drop or a byte drop
                // depending on whether the teardown had finished yet.
                server_->statsRef().bytesDropped.fetch_add(f.size(),
                                                           std::memory_order_relaxed);
                server_->statsRef().queueDrops.fetch_add(1, std::memory_order_relaxed);
                return;
            }

            // 全帧原子写入：容量不足则整帧丢弃（不破坏帧流）
            if (c->wbuf.writable() < f.size()) {
                c->pendingBytes.fetch_sub(f.size(), std::memory_order_relaxed);
                server_->statsRef().queueDrops.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            c->wbuf.write(f.data(), f.size());
            // No release here: this is step 2 above, the byte count moving from
            // "reserved by this task" to "reserved in wbuf". The drop branch just
            // above does subtract, and the drain in handleWrite subtracts again
            // later -- so "unchanged" is a statement about this path, not about
            // the counter's behaviour anywhere else.
            // 不判断"是否由空变非空"：armWrite 本身是幂等的目标状态迁移，重复
            // arm 无副作用；而 wbuf.empty() 看不到 pendingBytes 里"已占额、尚未
            // 写入"的那部分，用它决定是否 arm 会在与 disarmWrite 交错时漏掉真正的
            // 写兴趣，导致已入 wbuf 的帧无限滞留。
            // framesSent 不在这里计：帧在离开 wbuf 进内核时才计（见
            // drainWbufToSocket），与 bytesSent 同一计量点。
            armWrite(c);
        };
    } catch (...) {
        return false;
    }

    return enqueue(std::move(task));
}

inline bool SubReactor::submitClose(uint64_t id) noexcept {
    std::function<void()> task;
    try {
        task = [this, id]() noexcept {
            auto it = conns_.find(id);
            if (it != conns_.end()) {
                // Copy out and reset the map handle before closing: the deferred
                // close callbacks must not run while this frame holds the conn.
                std::shared_ptr<Conn> c = it->second;
                Conn* raw = c.get();
                c.reset();
                // Default (graceful): this is an application-level "we are done",
                // so the reply queued just before it must still reach the peer --
                // and so must answers to requests the peer already sent but this
                // reactor has not read yet.
                drainInputThenClose(raw);
            }
            flushCloseCallbacks();
        };
    } catch (...) { return false; }

    return enqueue(std::move(task));
}

inline void SubReactor::armWrite(Conn* c) noexcept {
    // Nothing asked for can ever leave a dead write side, and asking for it each
    // time bytes arrive would spin this reactor on level-triggered EPOLLOUT for
    // the rest of the connection's life.
    if (c->writeArmed || c->fd < 0) return;
    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLOUT; ev.data.fd = c->fd;
    if (epoll_ctl(epollFd_, EPOLL_CTL_MOD, c->fd, &ev) == 0) {
        c->writeArmed = true;
        return;
    }
    if (errno == ENOENT) {
        // Registered set out of sync (fd was re-added elsewhere): restore it with
        // ADD and adopt the write interest in the same call.
        if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, c->fd, &ev) == 0) {
            c->writeArmed = true;
            return;
        }
    }
    server_->statsRef().epollCtlErrors.fetch_add(1, std::memory_order_relaxed);
    closeConn(c, "epoll write registration failed", false);
}

inline void SubReactor::disarmWrite(Conn* c) noexcept {
    // Targeted on the EPOLLOUT bit, not on an "armed" flag: after our FIN the
    // write interest is gone but writeArmed is still true, and forgetting to
    // clear EPOLLOUT there would make a half-closed connection spin on a
    // level-triggered event it can never consume.
    if (!c->writeArmed || c->fd < 0) return;
    // Still has bytes to flush: keep the interest instead of undoing a preceding
    // armWrite. The caller is responsible for retrying the drain.
    struct epoll_event ev{};
    ev.events = EPOLLIN; ev.data.fd = c->fd;
    if (epoll_ctl(epollFd_, EPOLL_CTL_MOD, c->fd, &ev) < 0 && errno != ENOENT) {
        server_->statsRef().epollCtlErrors.fetch_add(1, std::memory_order_relaxed);
        closeConn(c, "epoll read registration failed", false);
        return;
    }
    c->writeArmed = false;
}

inline DrainOutcome SubReactor::drainWbufToSocket(Conn* c) noexcept {
    size_t budget = server_->cfg_.writeBudgetBytes;
    while (!c->wbuf.empty()) {
        if (!budget) return DrainOutcome::Stalled;
        auto s = c->wbuf.spans();
        // Send both wrapped ring segments in one syscall, within the fair-share
        // byte budget. A larger configured budget trades latency for throughput.
        iovec spans[2]{};
        spans[0].iov_base = const_cast<uint8_t*>(s.p1);
        spans[0].iov_len = std::min(s.n1, budget);
        spans[1].iov_base = const_cast<uint8_t*>(s.p2);
        spans[1].iov_len = std::min(s.n2, budget - spans[0].iov_len);
        msghdr msg{};
        msg.msg_iov = spans;
        msg.msg_iovlen = spans[1].iov_len ? 2 : 1;
        ssize_t k = ::sendmsg(c->fd, &msg, MSG_NOSIGNAL);
        if (k < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return DrainOutcome::Stalled;
            // EPIPE: this socket will never take another byte. Returning is not
            // the same thing as EAGAIN above, and the callers are the ones that
            // have to know the difference -- see handleWrite and parkClose.
            if (errno == EPIPE) {
                server_->statsRef().epipeWrites.fetch_add(1, std::memory_order_relaxed);
                return DrainOutcome::Dead;
            }
            return DrainOutcome::Error;
        }
        if (k == 0) return DrainOutcome::Error;
        // Frame accounting runs before consume(): the readable region still
        // holds the bytes that just left, so a frame starting inside this
        // chunk still has its header there to read. A frame is counted when
        // its last byte reaches the kernel -- the same boundary bytesSent
        // counts at, which is what keeps the two reconcilable. Counting on
        // queueing instead meant an abort could drop frames the counter had
        // already claimed, with nothing left to subtract them from.
        {
            size_t off  = 0;   // where the current frame starts, from the head
            size_t left = static_cast<size_t>(k);
            while (left > 0) {
                if (c->sendFrameRemain == 0) {
                    // Whole frames only ever enter wbuf, so a boundary inside
                    // the readable region always has a full header behind it.
                    uint8_t hdr[FRAME_HEADER_SIZE];
                    c->wbuf.peekAt(off, hdr, FRAME_HEADER_SIZE);
                    const uint32_t flen = (static_cast<uint32_t>(hdr[4]) << 24)
                                        | (static_cast<uint32_t>(hdr[5]) << 16)
                                        | (static_cast<uint32_t>(hdr[6]) <<  8)
                                        |  static_cast<uint32_t>(hdr[7]);
                    c->sendFrameRemain = FRAME_HEADER_SIZE + flen;
                }
                const size_t take = std::min(left, c->sendFrameRemain);
                c->sendFrameRemain -= take;
                left -= take;
                off += take;
                if (c->sendFrameRemain == 0)
                    server_->statsRef().framesSent.fetch_add(1,
                                                              std::memory_order_relaxed);
            }
        }
        c->wbuf.consume(static_cast<size_t>(k));
        budget -= static_cast<size_t>(k);
        c->pendingBytes.fetch_sub(static_cast<size_t>(k), std::memory_order_relaxed);
        server_->statsRef().bytesSent.fetch_add(static_cast<uint64_t>(k),
                                                  std::memory_order_relaxed);
    }
    return DrainOutcome::Drained;
}

// 读解析交错 + 每轮预算：防止单个客户端独占 reactor，也避免"rbuf 满含完整帧被误断"
inline int SubReactor::handleRead(Conn* c) noexcept {
    unscheduleRead(c);
    constexpr int kReadBudget  = 64 * 1024;
    constexpr int kFrameBudget = 256;
    int bytesLeft = kReadBudget, framesLeft = kFrameBudget;
    uint8_t tmp[16 * 1024];
    // Frames this call handed to the handler. Every exit reports it, because
    // "the peer asked nothing more" and "the peer asked, and answers for it are
    // now queued behind whichever task called us" are not distinguishable from
    // outside -- and only the second one has something to wait for.
    auto dispatched = [&framesLeft] { return kFrameBudget - framesLeft; };

    for (;;) {
        if (!server_->isRunning()) return dispatched();
        // 先解析已有完整帧
        parseFrames(c, framesLeft);
        if (c->isClosed.load(std::memory_order_acquire)) return dispatched();
        if (framesLeft <= 0 || bytesLeft <= 0) {
            scheduleRead(c);
            return dispatched();
        }

        if (c->rbuf.writable() == 0) {
            // 解析后仍满 → 半包过大，断开
            closeConn(c, "rbuf full with incomplete frame", false);
            return dispatched();
        }

        size_t want = std::min({sizeof(tmp), c->rbuf.writable(), static_cast<size_t>(bytesLeft)});
        ssize_t n = ::recv(c->fd, tmp, want, 0);
        if (n > 0) {
            c->rbuf.write(tmp, static_cast<size_t>(n));
            bytesLeft -= static_cast<int>(n);
            server_->statsRef().bytesRecv.fetch_add(static_cast<uint64_t>(n),
                                                      std::memory_order_relaxed);
        } else if (n == 0) {
            // Peer's FIN. If we still owe it a reply, closing here would turn the
            // last answer into an RST -- the client asked, we processed, and it
            // never sees the result. Park the close instead where we can: after
            // our own half-close, or (for an application close) with the FIN to
            // go out once wbuf drains.
            server_->statsRef().eofReads.fetch_add(1, std::memory_order_relaxed);
            c->peerClosed = true;
            parseFrames(c, framesLeft);
            if (c->isClosed.load(std::memory_order_acquire)) return dispatched();
            // No "budget spent, frames still buffered" case can exist here.
            // Every path that reaches recv() has already established that fewer
            // complete frames are buffered than the budget allows: parseFrames
            // at the top of the loop would otherwise have driven framesLeft to
            // 0 and exited before the read. recv() returning 0 adds no bytes,
            // so the parse above consumed nothing and framesLeft still holds.
            // (Measured: eofReads stays at 1 across bursts of 1..2000 frames,
            // which is what makes it usable as the busy-poll probe of P1-1b.)
            assert(framesLeft > 0);
            finishPeerClosed(c);
            return dispatched();
        } else {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                parseFrames(c, framesLeft);  // 再解析一次
                return dispatched();
            }
            closeConn(c, "recv error", false); return dispatched();
        }
    }
}

inline void SubReactor::handleWrite(Conn* c) noexcept {
    // Past our FIN there is nothing left to write, and the EPOLLOUT bit is kept
    // clear, so an event arriving here is stale: just make sure it is off.
    if (c->finSent) {
        disarmWrite(c); return;
    }
    const DrainOutcome r = drainWbufToSocket(c);
    if (r == DrainOutcome::Error) {
        closeConn(c, "send error", false); return;
    }
    // Nothing more will ever leave this socket, so there is nothing for the
    // rest of this function to wait for: no FIN can be negotiated (it requires
    // an empty wbuf first) and no later EPOLLOUT can deliver what is queued.
    // Ending it here rather than waiting is also what keeps this from becoming
    // the write-side echo of the busy-poll the read side was cured of -- a
    // socket with send-buffer space and a broken peer reports EPOLLOUT on every
    // single epoll_wait, and leaving the interest armed would have this reactor
    // re-run a failing sendmsg until some outside deadline noticed.
    if (r == DrainOutcome::Dead) {
        closeConn(c, "write side dead", false);
        return;
    }
    if (!c->wbuf.empty()) return;   // fair-share budget or EAGAIN: EPOLLOUT refires

    // A parked peer-close finishes here, deterministically, instead of waiting
    // for the next spurious EPOLLIN to notice the drain completed.
    if (c->peerClosed) {
        finishPeerClosed(c);
        return;
    }

    // wbuf is empty, so anything owed to the peer has reached the kernel: this is
    // the only place a FIN may be sent. A non-closing connection just drops its
    // write interest here; a parked close turns into a half-close.
    disarmWrite(c);
    if (c->closing && !tryBeginWriteShutdown(c)) {
        // shutdown() failed on a socket that was writable a moment ago: the only
        // way left to end this connection is a reset, which is at least honest.
        teardownClosed(c, "shutdown failed", false);
    }
}

// Sends FIN and records it. Reads stay armed: the connection is now waiting for
// the peer's own FIN (or closeLingerMs, enforced by doTick).
inline bool SubReactor::tryBeginWriteShutdown(Conn* c) noexcept {
    if (c->finSent || c->fd < 0) return c->finSent;
    // Never FIN over bytes still in wbuf: they would be lost, and the peer would
    // see a clean end to a truncated stream.
    if (!c->wbuf.empty()) return false;
    if (::shutdown(c->fd, SHUT_WR) < 0) return false;
    c->finSent = true;
    c->finSentAtMs = nowMs();
    return true;
}

// The peer has finished sending (recv returned 0). handleRead reaches this only
// once no complete frame is left unprocessed -- but "every request has run its
// handler" is NOT the same as "every reply is in wbuf": submitSend queues a
// task, so the answers produced by this very batch are still behind us in the
// queue, reserved in pendingBytes but not yet written. Tearing down here sends
// an orderly FIN over replies nobody ever sees (case E: 0 of 229432 bytes).
//
// What happens next depends on what is still owed, in the order that keeps the
// FIN behind every byte:
//   1. wbuf holds bytes     -- park, and drop EPOLLIN while parked: a FIN makes
//      the socket readable for the rest of its life, so with level-triggered
//      read interest still armed every epoll_wait returns at once and the
//      reactor busy-polls for the whole drain (measured: +35k FIN wakes in
//      250ms). handleWrite resumes this once wbuf empties.
//   2. wbuf empty but pendingBytes > 0 -- replies are queued behind this batch.
//      Defer exactly one queue round (waitingOnReplies) so they land in wbuf
//      first; FIFO runs every task queued before this one first. At most once:
//      a producer racing the close must not hold the connection open forever.
//   3. nothing outstanding  -- FIN (if we are closing) and tear down now.
//
// This is also the last chance to end the connection. Until it ran, an EOF with
// nothing else outstanding left the socket registered and idle: the read event
// had already fired, so nothing was going to fire again, and the connection sat
// in conns_ until doTick's linger deadline reached it -- which aborts, because a
// deadline is all it can be sure of. Every half-close initiated by the peer paid
// for that with an RST it never earned (P0-5).
inline void SubReactor::finishPeerClosed(Conn* c) noexcept {
    if (c->isClosed.load(std::memory_order_acquire)) return;
    if (!c->wbuf.empty()) {
        // Parked with the write side still productive. Read interest is dropped
        // for the rest of the drain (see the header comment): the only thing a
        // read could deliver is the FIN we have already seen.
        if (c->fd >= 0) {
            struct epoll_event wev{};
            wev.events  = EPOLLOUT;
            wev.data.fd = c->fd;
            if (epoll_ctl(epollFd_, EPOLL_CTL_MOD, c->fd, &wev) == 0)
                c->writeArmed = true;
            else
                server_->statsRef().epollCtlErrors.fetch_add(1,
                    std::memory_order_relaxed);
            // A failed MOD keeps EPOLLIN armed: the drain spins, nothing breaks.
        }
        return;                    // parked close finishes in handleWrite
    }
    if (!c->closing && !c->waitingOnReplies &&
        c->pendingBytes.load(std::memory_order_relaxed) > 0) {
        c->waitingOnReplies = true;
        const uint64_t id = c->id;
        if (enqueue([this, id]() noexcept {
                auto it = conns_.find(id);
                if (it == conns_.end()) return;
                // Copy the pointer, then drop this frame's reference before the
                // resumed check: it may tear the connection down.
                std::shared_ptr<Conn> keep = it->second;
                Conn* raw = keep.get();
                keep.reset();
                finishPeerClosed(raw);
            })) return;
        // The hand-off failed (queue full, or the reactor is going down): a
        // late FIN beats an answer nobody can wait for any longer.
    }
    if (c->closing && !c->finSent) {
        // Still closing on our side and the peer declared itself done too: the
        // only thing left is the FIN. Anything a handler queued was drained above
        // or is still passing through the socket buffer, ahead of it.
        if (!tryBeginWriteShutdown(c)) {
            teardownClosed(c, "peer closed", false); return;
        }
    }
    teardownClosed(c, "peer closed", true);
}

inline void SubReactor::parseFrames(Conn* c, int& framesLeft) noexcept {
    uint8_t hdr[FRAME_HEADER_SIZE];
    while (server_->isRunning() && framesLeft > 0 && c->rbuf.readable() >= FRAME_HEADER_SIZE) {
        c->rbuf.peek(hdr, FRAME_HEADER_SIZE);
        uint16_t magic = (static_cast<uint16_t>(hdr[0]) << 8) | hdr[1];
        if (magic != FRAME_MAGIC) {
            server_->statsRef().parseErrors.fetch_add(1, std::memory_order_relaxed);
            closeConn(c, "bad magic", false); return;
        }
        FrameType type = static_cast<FrameType>((static_cast<uint16_t>(hdr[2]) << 8) | hdr[3]);
        uint32_t len = (static_cast<uint32_t>(hdr[4]) << 24)
                     | (static_cast<uint32_t>(hdr[5]) << 16)
                     | (static_cast<uint32_t>(hdr[6]) <<  8)
                     |  static_cast<uint32_t>(hdr[7]);
        if (len > MAX_FRAME_PAYLOAD) {
            server_->statsRef().parseErrors.fetch_add(1, std::memory_order_relaxed);
            closeConn(c, "frame too large", false); return;
        }
        if (c->rbuf.readable() < FRAME_HEADER_SIZE + len) return;  // 半包，等更多

        c->rbuf.consume(FRAME_HEADER_SIZE);
        c->lastRecvMs = nowMs();
        server_->statsRef().framesRecv.fetch_add(1, std::memory_order_relaxed);
        --framesLeft;

        if (type == FrameType::PING) {
            const auto& pong = pongFrame();
            size_t pending = c->pendingBytes.load(std::memory_order_relaxed);
            bool reserved = false;
            while (pending <= server_->cfg_.maxPendingBytes &&
                   pong.size() <= server_->cfg_.maxPendingBytes - pending) {
                if (c->pendingBytes.compare_exchange_weak(pending, pending + pong.size(),
                                                          std::memory_order_relaxed,
                                                          std::memory_order_relaxed)) {
                    reserved = true;
                    break;
                }
            }
            if (!reserved || c->wbuf.writable() < pong.size()) {
                if (reserved) c->pendingBytes.fetch_sub(pong.size(),
                                                        std::memory_order_relaxed);
                // 全帧规则同样适用：PONG 发不出去 → 对端长期不读，断开
                // 强制断开：对端已经没有接收能力，再排空只是拖长描述符占用。
                closeConn(c, "wbuf full on pong", false); return;
            }
            if (c->closing) {
                // Mid-close: the FIN is already (or about to be) behind everything
                // we owe the peer. Queueing a PONG after it would either be sent
                // past the FIN -- which the kernel refuses -- or silently lost.
                // The peer is entitled to nothing more than the reply it asked for.
                c->pendingBytes.fetch_sub(pong.size(), std::memory_order_relaxed);
                server_->statsRef().framesDroppedOnClose.fetch_add(1, std::memory_order_relaxed);
                c->rbuf.consume(len);
                continue;
            }
            c->wbuf.write(pong.data(), pong.size());
            // framesSent 同样在离开 wbuf 时计（见 submitSend 的说明）。
            // 幂等的目标状态迁移；不用 "wasEmpty" 判断（见 submitSend 注释）
            armWrite(c);
            // closeConn 只标记 + 摘除，不再在栈内跑回调；但可能已因 arm 失败关闭
            if (c->isClosed.load(std::memory_order_acquire)) return;
            c->rbuf.consume(len);
        } else if (type == FrameType::PONG) {
            c->rbuf.consume(len);
        } else {
            if (len > 0) {
                if (len > frameScratch_.size()) {
                    closeConn(c, "scratch too small", false); return;
                }
                c->rbuf.peek(frameScratch_.data(), len);
            }
            c->rbuf.consume(len);
            auto& mh = server_->msgHandlerRef();
            if (mh) mh(c->id, frameScratch_.data(), len);
        }
    }
}

// Single teardown path for "this fd must no longer be visible to this reactor".
// Registration failures used to roll back by hand, and missed pieces: one path
// closed the descriptor but left the epoll registration behind, so a recycled fd
// number could arrive with stale interest attached. Every abort/close path goes
// through here so those three steps can never drift apart again.
//
// graceful decides one thing only: whether the close asks for an RST. An orderly
// close relies on the FIN that the caller already sent via shutdown(SHUT_WR).
inline void SubReactor::unregisterConn(Conn* c, bool graceful) noexcept {
    if (c->fd < 0) return;
    // An abort asks for the reset explicitly instead of relying on whatever the
    // close() picks: SO_LINGER{1,0} drops the send buffer and sends RST. It is
    // set on abort only -- never on close normally, or every orderly close would
    // become a reset.
    if (!graceful) {
        struct linger lg{};
        lg.l_onoff  = 1;
        lg.l_linger = 0;
        setsockopt(c->fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    }
    fdIndex_.erase(c->fd);
    epoll_ctl(epollFd_, EPOLL_CTL_DEL, c->fd, nullptr);
    ::close(c->fd);
    c->fd = -1;
    c->writeArmed = false;
    c->closing  = false;
}

// pendingBytes is reserved at enqueue time and released on two paths only:
// a successful sendmsg (drainWbufToSocket) and an explicit whole-frame drop
// (capacity reject in submitSend / PONG). A closing connection takes a third,
// previously missing path: the bytes still sitting in wbuf are never sent, so
// they must be subtracted here or the reservation leaks for the lifetime of the
// connection -- and any watermark built on it stays permanently inflated.
//
// Bytes still reserved by queued-but-unwritten send tasks are NOT handled here:
// submitSend already releases them on the isClosed path, so this function must
// never try to guess at them (it would double-count).
inline void SubReactor::discardPendingOutput(Conn* c) noexcept {
    const size_t remaining = c->wbuf.readable();
    if (remaining == 0) return;
    c->wbuf.clear();
    // The in-flight frame boundary dies with the buffer.
    c->sendFrameRemain = 0;
    c->pendingBytes.fetch_sub(remaining, std::memory_order_relaxed);
    server_->statsRef().bytesDropped.fetch_add(remaining, std::memory_order_relaxed);
}

// Close is split in two on purpose. closeConn() only does the work that is safe
// to do inside a handler stack frame: unlist, detach the descriptor, drop both
// maps' references, bump counters. It never destroys the connection (the caller
// holds a shared_ptr) and never invokes user callbacks. A graceful close is not
// even that much: it parks the connection and may not unlist it for a while.
//
// USER CALLBACKS ARE DEFERRED. ConnStateHandler(false) / CloseHandler may call
// stop() (allowed) or release the last external owner of MultiReactorServer; if
// they ran here, the stack frame would resume on a destructed object -- this
// reactor in particular, since cleanup()/stop() continue running from closeConn.
// flushCloseCallbacks() drains them from a frame that owns no reference to the
// closed connection, after the caller's keepAlive is gone.
//
// Two ways out of a connection, and the difference is visible on the wire:
//   closeConn(..., graceful=true)  -- default. Called when the peer, the
//       application or the server asked to end an otherwise healthy connection.
//       Everything owed to the peer is delivered first: see parkClose().
//   closeConn(..., graceful=false) -- abort. Pending output is dropped and
//       SO_LINGER{on,0} asks the kernel for an RST, so the peer cannot mistake a
//       half-processed stream for a complete one.
inline void SubReactor::closeConn(Conn* c, const char* reason, bool graceful) noexcept {
    // A graceful close of a live connection is parked; one that has already sent
    // its FIN (or is aborted) is torn down right here.
    if (graceful && !c->finSent) {
        parkClose(c, reason); return;
    }
    teardownClosed(c, reason, graceful);
}

// Why the close cannot simply be "write what is queued, then close":
//   1. wbuf lives in user space. shutdown() takes effect immediately and closes
//      the kernel's send direction, so anything still in wbuf when it is called
//      is lost for good.
//   2. close() on a socket whose peer data is unread sends RST and discards the
//      queued output. The peer's last request is often still unread here.
// So: drain wbuf into the kernel first, then shutdown(SHUT_WR); the kernel keeps
// delivering what it already accepted, in order, before the FIN. The socket is
// closed only after the peer's FIN arrives (doTick bounds the wait).
//
// A graceful close while the connection is still alive on the wire. Until the
// peer has seen both everything we owed it and our FIN the connection is marked
// `closing`, so no further submissions are accepted -- but the reply queued just
// before the close still is (see submitSend).
//
// Idempotent: a second close request (or an idle-timeout sweep firing while the
// first FIN is still in flight) must not restart the drain or skip it.
inline void SubReactor::parkClose(Conn* c, const char* reason) noexcept {
    if (c->isClosed.load(std::memory_order_acquire) || c->finSent) return;
    if (!c->closing) {
        c->closing = true; c->closeStartedAtMs = nowMs();
    }
    // Hand over what is already queued; whatever does not fit in the socket
    // buffer stays in wbuf and is finished by handleWrite (EPOLLOUT armed).
    const DrainOutcome r = drainWbufToSocket(c);
    if (r == DrainOutcome::Error) {
        teardownClosed(c, reason, false); return;
    }
    // Once the socket refuses writes for good there is no graceful close left to
    // honour -- nothing owed can leave, so no FIN can honestly follow it. Waiting
    // would change nothing except which deadline does this same abort later.
    if (r == DrainOutcome::Dead) {
        teardownClosed(c, reason, false); return;
    }
    if (!c->wbuf.empty()) {
        armWrite(c); return;
    }
    // wbuf empty: either every byte is in the socket buffer, or sendmsg stopped
    // exactly on the budget boundary. In both cases nothing more can be queued
    // (`closing` refuses submissions), so the FIN goes out now.
    tryBeginWriteShutdown(c);
}

// closeConnection() asked to hang up while the peer's request may still be
// unread in the socket buffer. Those bytes arrived before this close did, so
// answers to them are part of what "graceful" promises the peer: read and serve
// them, then FIN.
//
// Skipping this still parses every frame and still runs the handler, so the
// application believes it answered -- then submitSend refuses the replies
// because `closing` is set, counts them into bytesDropped, and the peer receives
// an orderly FIN over requests nobody ever answered. Nothing says so out loud:
// framesRecv goes up, the handler ran, and only the byte count hints that a
// request was taken in and served nothing.
//
// The FIN cannot go out from this frame even though the drain just finished.
// Those replies are queued as tasks, behind this one, so wbuf reads empty here
// and parkClose would half-close ahead of them. Resuming one queue round later
// is what puts the answers in front of the FIN. It resumes at most once: a peer
// that keeps sending would otherwise hold the close open with more input.
inline void SubReactor::drainInputThenClose(Conn* c) noexcept {
    if (c->isClosed.load(std::memory_order_acquire) || c->finSent) return;

    const int served = handleRead(c);
    // handleRead has its own exits: the peer's FIN may have arrived in the same
    // read, or the socket may have errored. Either way the connection is already
    // accounted for and there is nothing left for this request to do.
    if (c->isClosed.load(std::memory_order_acquire) || c->finSent) return;

    // Nothing further to read, yet bytes are still reserved: replies whose
    // handler already ran -- possibly two batches apart, with this task landing
    // between them. Parking now side-effects every one of them into `closing`,
    // which submitSend refuses, and the peer gets an orderly FIN over a request
    // we took in, handled, and answered into nothing (case F: 11 of 22 bytes).
    // Their reservations still standing is what makes them visible here.
    if (served == 0 && !c->waitingOnReplies &&
        c->pendingBytes.load(std::memory_order_relaxed) > 0) {
        c->waitingOnReplies = true;
        const uint64_t id = c->id;
        if (enqueue([this, id]() noexcept {
                auto it = conns_.find(id);
                if (it == conns_.end()) return;
                std::shared_ptr<Conn> keep = it->second;
                Conn* raw = keep.get();
                keep.reset();
                parkClose(raw, "requested");
            })) return;
        // The hand-off failed: park here rather than keeping it forever.
    }
    if (served == 0 || c->waitingOnReplies) {
        parkClose(c, "requested");
        return;
    }

    c->waitingOnReplies = true;
    const uint64_t id = c->id;
    if (enqueue([this, id]() noexcept {
            auto it = conns_.find(id);
            if (it == conns_.end()) return;
            // Release the map's reference only after copying the pointer, so this
            // frame holds nothing while parkClose may close the connection.
            std::shared_ptr<Conn> keep = it->second;
            Conn* raw = keep.get();
            keep.reset();
            parkClose(raw, "requested");
        })) return;

    // Nothing came of the hand-off -- queue full, or the reactor is going down.
    // A late close beats an answer nobody would wait for, so honour it here.
    parkClose(c, "requested");
}

// Unlist -> detach descriptor -> account -> queue deferred callbacks. Shared by
// the graceful and abort exits so the two can never disagree on counters.
// `graceful=false` closes with SO_LINGER{on,0}: an explicit RST instead of the
// FIN default, so a peer is never handed a truncated stream that looks complete.
inline void SubReactor::teardownClosed(Conn* c, const char* reason, bool graceful) noexcept {
    bool expected = false;
    if (!c->isClosed.compare_exchange_strong(expected, true,
                                             std::memory_order_acq_rel))
        return;
    // reason is not stored on the connection: it is forwarded straight into the
    // deferred callback below, which copies it. Keeping it here too buys nothing
    // and lasts exactly as long as the caller's string does -- a trap for the
    // next person to add a reader after the teardown has returned.
    unscheduleRead(c);
    // Anything still in wbuf will never be sent. Reconcile the byte reservation
    // with the buffer that backs it, or pendingBytes never drains for this
    // connection and any watermark derived from it leaks. Accounted before the
    // close so a close callback reading stats sees it.
    discardPendingOutput(c);

    std::shared_ptr<Conn> keepAlive;
    {
        auto it = conns_.find(c->id);
        if (it != conns_.end()) {
            keepAlive = it->second; conns_.erase(it);
        }
    }
    unregisterConn(c, graceful);   // fdIndex_ erase + EPOLL_CTL_DEL + close + fd = -1

    server_->statsRef().closed.fetch_add(1, std::memory_order_relaxed);
    if (!graceful) server_->statsRef().aborted.fetch_add(1, std::memory_order_relaxed);
    server_->onConnectionClosed(c->id);
    c->admission.reset();

    // Queue the deferred notification. The Conn stays alive inside
    // pendingCloses_ until flushCloseCallbacks() drops it, so a callback never
    // observes a connection that has already been destroyed. peer/reason are
    // copied out: `reason` is borrowed for this call only and must not outlive
    // the caller's frame.
    pendingCloses_.push_back(keepAlive);
    pendingCloseIds_.push_back(c->id);
    pendingClosePeers_.push_back(c->peer);
    pendingCloseReasons_.push_back(reason ? reason : "");
}

// Release our reference before running the callbacks: after this the connection
// may be fully destroyed, which is exactly the state a deferred callback expects.
// Takes a mutable reference on purpose: reset() cannot be called through const,
// and the caller's slot must be emptied rather than merely read.
inline void SubReactor::finishClose(std::shared_ptr<Conn>& c) noexcept {
    c.reset();
}

// Runs the deferred ConnStateHandler(false) / CloseHandler pairs. The vectors are
// swapped out first, so a callback that closes more connections (or enqueues
// work) lands in a fresh batch instead of invalidating this loop.
//
// All four vectors are cleared together so their indices always agree.
inline void SubReactor::flushCloseCallbacks() noexcept {
    std::vector<std::shared_ptr<Conn>> live;
    std::vector<uint64_t>    ids;
    std::vector<std::string> peers;
    std::vector<std::string> reasons;
    live.swap(pendingCloses_);
    ids.swap(pendingCloseIds_);
    peers.swap(pendingClosePeers_);
    reasons.swap(pendingCloseReasons_);
    if (ids.empty()) return;

    for (size_t i = 0; i < ids.size(); ++i) {
        if (i < live.size()) finishClose(live[i]);   // connection may die here
        auto& sh = server_->stateHandlerRef();
        if (sh) sh(ids[i], peers[i], false);
        if (server_->closeHandler_) server_->closeHandler_(ids[i], peers[i], reasons[i].c_str());
    }
}

inline void SubReactor::scheduleRead(Conn* c) noexcept {
    if (c->readPending) return;
    c->readPending = true;
    c->readPrev = readTail_;
    if (readTail_) readTail_->readNext = c;
    else readHead_ = c;
    readTail_ = c;
}

inline void SubReactor::unscheduleRead(Conn* c) noexcept {
    if (!c->readPending) return;
    if (c->readPrev) c->readPrev->readNext = c->readNext;
    else readHead_ = c->readNext;
    if (c->readNext) c->readNext->readPrev = c->readPrev;
    else readTail_ = c->readPrev;
    c->readPrev = c->readNext = nullptr;
    c->readPending = false;
}

inline void SubReactor::doTick() noexcept {
    const int64_t now   = nowMs();
    const int64_t cutoff = now - server_->idleTimeoutMs();
    const int64_t linger = server_->closeLingerMs();
    // Every due close is collected during the walk and performed after it,
    // because closeConn is not always content to park the connection. Three
    // ways in reach from a single call all erase from conns_ -- the map this
    // range-for is holding an iterator into:
    //   - a connection that has already sent its FIN goes straight to
    //     teardownClosed (parkClose returns early on finSent);
    //   - parkClose's own drain may fail (a peer that vanished and RST'd) and
    //     takes the same teardown;
    //   - armWrite/disarmWrite abort the connection if epoll_ctl fails.
    // None of them is reachable from the sweep today on purpose; all of them
    // are reachable by accident. Moving the closes out of the loop makes the
    // question moot instead of documented, which is stronger than the assert
    // that would otherwise stand in for it -- an assert is compiled out under
    // NDEBUG, leaving production with nothing.
    //
    // The connections are carried as shared_ptr: erase detaches them from
    // conns_ between being chosen and being closed, and nothing else holds a
    // reference across that gap.
    std::vector<std::shared_ptr<Conn>> idle;      // graceful: may still owe bytes
    std::vector<std::shared_ptr<Conn>> expired;   // past its deadline: forced
    for (auto& kv : conns_) {
        Conn* c = kv.second.get();
        if (c->closing) {
            // A closing connection has a deadline regardless of which stage it
            // reached, and it is the same value for both: how long the close may
            // take. lastRecvMs is useless here (the peer has nothing left to
            // send), so measure from the close itself; parkClose stamps
            // closeStartedAtMs before any of the stages that can stall.
            if (linger > 0 && c->fd >= 0 && c->closeStartedAtMs != 0 &&
                now - c->closeStartedAtMs >= linger)
                expired.push_back(kv.second);
            continue;
        }
        if (c->lastRecvMs < cutoff) {
            server_->statsRef().idleTimeouts.fetch_add(1, std::memory_order_relaxed);
            // Default (graceful): an idle connection is still a legitimate peer
            // and may hold a half-finished exchange, so let the close park until
            // what we owe it is out.
            idle.push_back(kv.second);
        }
    }
    for (auto& c : idle)    closeConn(c.get(), "idle timeout");
    for (auto& c : expired) closeConn(c.get(), "close linger expired", false);
}

inline void SubReactor::loop() noexcept {
    MultiReactorServer::CallbackScope callbacks(server_);
    try {
        std::array<struct epoll_event, 1024> events{};
        auto lastTick = std::chrono::steady_clock::now();
        const int tickMs = server_->tickMs();

        while (running_.load(std::memory_order_acquire) && server_->isRunning()) {
#ifdef INDUSTRIAL_TCP_TESTING
            if (server_->failWorkerForTest.exchange(false)) throw 1;
#endif
            // Only visit queued continuations. A fixed batch bounds work even
            // when a busy connection immediately schedules itself again.
            for (size_t budget = 0; readHead_ && budget < 64; ++budget) {
                auto it = conns_.find(readHead_->id);
                if (it == conns_.end()) {
                    unscheduleRead(readHead_); continue;
                }
                auto c = it->second;
                handleRead(c.get());
            }
            int n = ::epoll_wait(epollFd_, events.data(), static_cast<int>(events.size()),
                                  readHead_ ? 0 : tickMs);
            if (n < 0) {
                if (errno == EINTR) continue;
                server_->statsRef().reactorFailures.fetch_add(1,
                                                               std::memory_order_relaxed);
                server_->fail();
                break;
            }
            for (int i = 0; i < n; ++i) {
                int      fd = events[i].data.fd;
                uint32_t ev = events[i].events;

                if (fd == wakeFd_) {
                    drainWakeup(); processPending(); continue;
                }

                auto it = fdIndex_.find(fd);
                if (it == fdIndex_.end()) continue;
                Conn* c = it->second;

                // ---- 关键修复：整个事件处理期间持有 shared_ptr 保活 ----
                std::shared_ptr<Conn> keepAlive;
                {
                    auto cit = conns_.find(c->id);
                    if (cit != conns_.end()) keepAlive = cit->second;
                }
                if (!keepAlive) continue;

                // HUP/ERR is the kernel telling us the socket is already broken
                // (or the peer is gone for good); draining into it cannot work.
                // Abort so the peer is not left waiting on a stream that will
                // never complete.
                if (ev & (EPOLLHUP | EPOLLERR)) {
                    closeConn(c, "hup/err", false); continue;
                }
                if (ev & EPOLLIN) {
                    handleRead(c);
                    if (c->isClosed.load(std::memory_order_acquire)) continue;
                }
                if (ev & EPOLLOUT) {
                    handleWrite(c);
                }
            }
            // Deferred close callbacks run with no per-connection stack frames
            // alive, so a callback may stop() or destroy the server safely.
            flushCloseCallbacks();
            processPending();
            flushCloseCallbacks();

            auto now = std::chrono::steady_clock::now();
            if (now - lastTick >= std::chrono::milliseconds(tickMs)) {
                lastTick = now;
                doTick();
            }
        }
    } catch (...) {
        server_->statsRef().reactorFailures.fetch_add(1, std::memory_order_relaxed);
        // Raise the failure flag only; the lifecycle state stays untouched so a
        // concurrent start() verdict is not overwritten by a worker crash.
        server_->fail();
    }
    // Reject submissions even when the loop failed rather than being stopped.
    {
        std::lock_guard<std::mutex> lk(pendingMutex_);
        running_.store(false, std::memory_order_release);
    }
    cleanup();
}

// ====================================================================
//                     MultiReactorServer 实现
// ====================================================================

inline bool MultiReactorServer::initListenSocket() noexcept {
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    char portStr[16];
    std::snprintf(portStr, sizeof(portStr), "%u", (unsigned)cfg_.port);
    const char* host = (cfg_.bindAddr.empty() || cfg_.bindAddr == "0.0.0.0")
                       ? nullptr : cfg_.bindAddr.c_str();
    struct addrinfo* res = nullptr;
    if (getaddrinfo(host, portStr, &hints, &res) != 0) return false;
    for (auto* p = res; p; p = p->ai_next) {
        int fd = ::socket(p->ai_family,
                          p->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                          p->ai_protocol);
        if (fd < 0) continue;
        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
        if (p->ai_family == AF_INET6) {
            int zero = 0;
            setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &zero, sizeof(zero));
        }
        if (::bind(fd, p->ai_addr, p->ai_addrlen) < 0) {
            ::close(fd); continue;
        }
        if (::listen(fd, cfg_.backlog) < 0) {
            ::close(fd); continue;
        }
        listenFd_ = fd; break;
    }
    freeaddrinfo(res);
    return listenFd_ >= 0;
}

inline bool MultiReactorServer::start() noexcept {
    if (callbackServer_ == this) return false;
    std::lock_guard<std::mutex> lifecycle(lifecycleMutex_);
    if (life_.load(std::memory_order_acquire) != Life::Stopped) return false;
    life_.store(Life::Starting, std::memory_order_release);
    failed_.store(false, std::memory_order_release);
    if (!reactors_.empty() || acceptThread_.joinable() ||
        cfg_.rbufCapacity < FRAME_HEADER_SIZE + MAX_FRAME_PAYLOAD ||
        cfg_.wbufCapacity < FRAME_HEADER_SIZE || cfg_.maxPendingBytes < FRAME_HEADER_SIZE ||
        cfg_.maxConnections <= 0 || cfg_.tickMs <= 0 || cfg_.idleTimeoutMs <= 0 ||
        cfg_.writeBudgetBytes == 0 || cfg_.closeLingerMs <= 0 || cfg_.backlog <= 0) {
        life_.store(Life::Stopped, std::memory_order_release);
        return false;
    }

    // Deadline granularity: both timeouts only fire on tick boundaries, so a
    // tick coarser than the deadline silently stretches it by up to a full
    // tick. Warn rather than refuse -- a coarse tick with long deadlines is a
    // legitimate choice; a coarse tick with a 200ms promise is a surprise.
    if (cfg_.tickMs > cfg_.closeLingerMs || cfg_.tickMs > cfg_.idleTimeoutMs) {
        std::fprintf(stderr,
                     "[warn] tickMs=%d is coarser than closeLingerMs=%d / "
                     "idleTimeoutMs=%d: both deadlines only fire on tick "
                     "boundaries, so each can land up to a full tick late\n",
                     cfg_.tickMs, cfg_.closeLingerMs, cfg_.idleTimeoutMs);
    }

    // 配置校验
    if (cfg_.maxPendingBytes > cfg_.wbufCapacity) {
        cfg_.maxPendingBytes = cfg_.wbufCapacity;
    }

    // ---- fd 上限：能抬就抬，抬不上去要说清楚能撑到多少 ----
    // Warning and starting anyway hands operations a server that accepts the
    // first thousand connections and then refuses everything without a word,
    // which is worse than refusing to boot: it looks healthy until it isn't.
    // Raising the soft limit is the one action here that cannot make things
    // worse -- the hard ceiling stays where the environment put it -- so try it
    // before reporting anything.
    struct rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        const uintmax_t needed = static_cast<uintmax_t>(cfg_.maxConnections) + 64;
        // Compare in uintmax_t, never int: RLIM_INFINITY is "no limit", and
        // narrowing it to int wraps negative, which read as "too low" and
        // printed a spurious warning on unlimited configurations.
        uintmax_t cur = (rl.rlim_cur == RLIM_INFINITY)
                        ? UINTMAX_MAX : static_cast<uintmax_t>(rl.rlim_cur);
        if (cur < needed && rl.rlim_cur < rl.rlim_max) {
            struct rlimit want = rl;
            want.rlim_cur = rl.rlim_max;
            if (setrlimit(RLIMIT_NOFILE, &want) == 0) {
                std::fprintf(stderr,
                    "[warn] raised RLIMIT_NOFILE %ju -> %ju for "
                    "maxConnections=%d\n",
                    static_cast<uintmax_t>(rl.rlim_cur), static_cast<uintmax_t>(rl.rlim_max),
                    cfg_.maxConnections);
                cur = (rl.rlim_max == RLIM_INFINITY)
                      ? UINTMAX_MAX : static_cast<uintmax_t>(rl.rlim_max);
            }
        }
        if (cur < needed) {
            std::fprintf(stderr,
                "[warn] RLIMIT_NOFILE=%ju (hard %ju) too low for "
                "maxConnections=%d: connections beyond roughly %ju will be "
                "refused, counted as acceptErrors\n",
                static_cast<uintmax_t>(rl.rlim_cur), static_cast<uintmax_t>(rl.rlim_max),
                cfg_.maxConnections,
                cur > 64 ? cur - 64 : 0);
        }
    }

    if (!initListenSocket()) {
        life_.store(Life::Stopped, std::memory_order_release); return false;
    }

    // ---- Running must be visible before any worker thread is constructed ----
    // Every worker asks isRunning() whether it should keep working, and every
    // worker asks it the instant its thread starts - long before this function
    // gets anywhere near returning. Publishing Running later is therefore a
    // race that each worker loses by default: it reads Starting, concludes the
    // server never came up, and returns without ever reaching epoll_wait.
    //
    // Two instances of exactly this have shipped. The accept loop read Starting
    // and fell through to its teardown, closing the listening socket while
    // start() still answered true (P0-3). The reactors survived only when they
    // happened to start slowly enough to see this store; a fast one exited with
    // running_ = false and then quietly refused every connection round-robin
    // handed it, which looked like nothing at all from the client side - the
    // socket was accepted and closed again immediately, no logs, no counters
    // anyone thinks to read.
    //
    // Everything above here already fails through the recovery path below and
    // reports false; everything below here is threads that read this value the
    // moment they come into existence. There is nothing left to wait for.
    life_.store(Life::Running, std::memory_order_release);

    // Same resolution as everywhere else; see Config::resolvedReactorCount for
    // why this is not computed here.
    int nReactors = cfg_.resolvedReactorCount();

    try {
        reactors_.reserve(nReactors);
    }
    catch (...) { ::close(listenFd_); listenFd_ = -1;
                  life_.store(Life::Stopped, std::memory_order_release); return false; }

    try {
    for (int i = 0; i < nReactors; ++i) {
        auto r = std::make_unique<SubReactor>(i, this);
        if (!r->start()) {
            for (auto& rr : reactors_) rr->stop();
            reactors_.clear();
            ::close(listenFd_); listenFd_ = -1;
            life_.store(Life::Stopped, std::memory_order_release); return false;
        }
        reactors_.push_back(std::move(r));
    }
    } catch (...) {
        for (auto& r : reactors_) r->stop();
        reactors_.clear();
        ::close(listenFd_); listenFd_ = -1;
        life_.store(Life::Stopped, std::memory_order_release); return false;
    }

    // ---- 启动同步：accept 线程报到后 start 才返回 ----
    // Success means "the accept loop is armed and no component has failed yet",
    // not "the lifecycle counter is still false". A component that died during
    // bring-up raises failed_ instead of rewriting the lifecycle state, so the
    // verdict returned here cannot be flipped out from under the caller.
    // Running itself was published further up, before any worker existed.
    try {
        auto ready = std::make_shared<std::promise<bool>>();
        auto fut = ready->get_future();
        acceptThread_ = std::thread([this, ready]() noexcept { acceptLoop(*ready); });
        if (fut.get()) {
            if (!failed_.load(std::memory_order_acquire)) {
                return true;
            }
        }
    } catch (...) {
    }

    life_.store(Life::Stopping, std::memory_order_release);
    if (acceptThread_.joinable()) acceptThread_.join();
    for (auto& r : reactors_) r->stop();
    reactors_.clear();
    if (wakeFd_ >= 0) {
        ::close(wakeFd_); wakeFd_ = -1;
    }
    if (epollFd_ >= 0) {
        ::close(epollFd_); epollFd_ = -1;
    }
    if (listenFd_ >= 0) {
        ::close(listenFd_); listenFd_ = -1;
    }
    failed_.store(false, std::memory_order_release);
    life_.store(Life::Stopped, std::memory_order_release);
    return false;
}

inline void MultiReactorServer::stop() noexcept {
    if (callbackServer_ == this) {
        requestStop(); return;
    }
    std::lock_guard<std::mutex> lifecycle(lifecycleMutex_);
    // A second stop() (or a stop() after a failed start) must not rerun recovery:
    // it would reopen the submission barrier and join threads that are not there.
    if (life_.load(std::memory_order_acquire) == Life::Stopped) return;
    life_.store(Life::Stopping, std::memory_order_release);
    {
        std::unique_lock<std::shared_mutex> submissions(submissionMutex_);
        // Drain callers that passed the running check before shutdown began.
    }

    // 先唤醒 accept 线程退出
    wakeAcceptLoop();
    if (acceptThread_.joinable()) acceptThread_.join();

    // accept 线程退出后，安全关闭其持有的 fd
    if (wakeFd_   >= 0) {
        ::close(wakeFd_);   wakeFd_   = -1;
    }
    if (epollFd_  >= 0) {
        ::close(epollFd_);  epollFd_  = -1;
    }
    if (listenFd_ >= 0) {
        ::close(listenFd_); listenFd_ = -1;
    }

    // 停所有 reactor（此时新的 sendTo / closeConnection 已被 isRunning() 拒绝）
    for (auto& r : reactors_) r->stop();
    reactors_.clear();

    std::unique_lock<std::shared_mutex> lk(connsMutex_);
    conns_.clear();
    failed_.store(false, std::memory_order_release);
    life_.store(Life::Stopped, std::memory_order_release);
}

inline void MultiReactorServer::wait() noexcept {
    while (isRunning())
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
}

inline void MultiReactorServer::wakeAcceptLoop() noexcept {
    // Only touches descriptors that the accept thread itself owns and closes
    // under acceptFdMutex_, so a stop racing with accept-loop teardown can never
    // write into a recycled fd number.
    std::lock_guard<std::mutex> descriptors(acceptFdMutex_);
    if (wakeFd_ >= 0) {
        uint64_t v = 1;
        ssize_t r = ::write(wakeFd_, &v, sizeof(v));
        (void)r;
    }
}

inline SubReactor* MultiReactorServer::pickReactor() noexcept {
    if (reactors_.empty()) return nullptr;
    size_t i = nextReactor_.fetch_add(1, std::memory_order_relaxed) % reactors_.size();
    return reactors_[i].get();
}

inline void MultiReactorServer::acceptLoop(std::promise<bool>& ready) noexcept {
    bool ok = false;
    try {
    {
    std::lock_guard<std::mutex> descriptors(acceptFdMutex_);
    epollFd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epollFd_ >= 0) {
        wakeFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (wakeFd_ >= 0) {
            struct epoll_event ev{};
            ev.events = EPOLLIN; ev.data.fd = listenFd_;
            if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, listenFd_, &ev) == 0) {
                ev.data.fd = wakeFd_;
                if (epoll_ctl(epollFd_, EPOLL_CTL_ADD, wakeFd_, &ev) == 0) {
                    ok = true;
                }
            }
        }
    }
    }

    try {
        ready.set_value(ok);
    } catch (...) {}
    // Descriptor setup failure is the accept loop's own failure: raise the shared
    // flag instead of forcing the whole lifecycle down, so start() reports false
    // through its normal recovery path rather than racing on the state.
    if (!ok) {
        fail(); return;
    }

    std::array<struct epoll_event, 64> events{};
    while (ok && isRunning()) {
#ifdef INDUSTRIAL_TCP_TESTING
        if (failAcceptForTest.exchange(false)) throw 1;
#endif
        int n = ::epoll_wait(epollFd_, events.data(), static_cast<int>(events.size()), 500);
        if (n < 0) {
            if (errno == EINTR) continue;
            stats_.reactorFailures.fetch_add(1, std::memory_order_relaxed);
            fail();
            break;
        }
        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            if (fd == wakeFd_) {
                uint64_t v = 0;
                ssize_t r = ::read(wakeFd_, &v, sizeof(v)); (void)r;
                continue;
            }
            if (fd != listenFd_) continue;

            // 有界 accept，避免 flood 饿死其它 fd
            constexpr int kMaxAcceptPerRound = 64;
            for (int k = 0; k < kMaxAcceptPerRound; ++k) {
                struct sockaddr_storage ss{};
                socklen_t slen = sizeof(ss);
                int cfd = ::accept4(listenFd_, (struct sockaddr*)&ss, &slen,
                                    SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (cfd < 0) {
                    if (errno == EINTR) continue;
                    // Out of descriptors is not an error to swallow and it is
                    // not one to spin on either: the listening socket stays
                    // readable, so returning to epoll_wait immediately means
                    // burning a core rejecting nothing. It is also the one
                    // accept failure that can be predicted at startup and
                    // therefore should be countable -- without a counter the
                    // only symptom is "it stopped accepting and said nothing".
                    if (errno == EMFILE || errno == ENFILE) {
                        stats_.acceptErrors.fetch_add(1, std::memory_order_relaxed);
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        continue;
                    }
                    break;
                }
                FdGuard cfdGuard(cfd);

                bool reject = false;
                {
                    if (admitted_.load() >= static_cast<size_t>(cfg_.maxConnections)) reject = true;
                }
                if (reject) {
                    stats_.rejected.fetch_add(1, std::memory_order_relaxed);
                    continue;   // cfdGuard 关闭
                }

                int one = 1;
                setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY,  &one, sizeof(one));
                setsockopt(cfd, SOL_SOCKET,  SO_KEEPALIVE, &one, sizeof(one));
                int ka_idle = 30, ka_intvl = 10, ka_cnt = 3;
                setsockopt(cfd, IPPROTO_TCP, TCP_KEEPIDLE,  &ka_idle,  sizeof(ka_idle));
                setsockopt(cfd, IPPROTO_TCP, TCP_KEEPINTVL, &ka_intvl, sizeof(ka_intvl));
                setsockopt(cfd, IPPROTO_TCP, TCP_KEEPCNT,   &ka_cnt,   sizeof(ka_cnt));

                try {
                char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
                std::string peer = "unknown";
                if (getnameinfo((struct sockaddr*)&ss, slen, hbuf, sizeof(hbuf),
                                sbuf, sizeof(sbuf),
                                NI_NUMERICHOST | NI_NUMERICSERV) == 0) {
                    try {
                        peer.assign(hbuf); peer.push_back(':'); peer.append(sbuf);
                    } catch (...) { peer = "unknown"; }
                }

                SubReactor* r = pickReactor();
                if (!r) continue;   // cfdGuard 关闭

                uint64_t id = nextConnId_.fetch_add(1, std::memory_order_relaxed);
                // Submission consumes ownership on both success and rejection.
                // accepted is NOT bumped here: queueing a task is not a connection.
                // The reactor bumps it once the socket is registered in both maps,
                // so a task that fails during registration counts as rejected only.
                auto admission = std::make_shared<Admission>(admitted_);
                if (!r->submitNewConnection(std::move(cfdGuard), std::move(peer), id,
                                          std::move(admission))) {
                    stats_.rejected.fetch_add(1, std::memory_order_relaxed);
                }
                } catch (...) {
                    // Before handoff, cfdGuard still owns the accepted socket.
                    stats_.rejected.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    }
    } catch (...) {
        stats_.reactorFailures.fetch_add(1, std::memory_order_relaxed);
        fail();
        try {
            ready.set_value(false);
        } catch (...) {}
    }
    // Close on every exit, including unexpected failure. The same mutex protects
    // stop's wake write, preventing descriptor reuse between its check and write.
    std::lock_guard<std::mutex> descriptors(acceptFdMutex_);
    if (wakeFd_ >= 0) {
        ::close(wakeFd_); wakeFd_ = -1;
    }
    if (epollFd_ >= 0) {
        ::close(epollFd_); epollFd_ = -1;
    }
    if (listenFd_ >= 0) {
        ::close(listenFd_); listenFd_ = -1;
    }
}

inline bool MultiReactorServer::onConnectionOpened(uint64_t id,
                                                   std::shared_ptr<Conn> c) noexcept {
    try {
        std::unique_lock<std::shared_mutex> lk(connsMutex_);
        conns_[id] = std::move(c);
        return true;
    } catch (...) {
        stats_.queueDrops.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
}

inline void MultiReactorServer::onConnectionClosed(uint64_t id) noexcept {
    std::unique_lock<std::shared_mutex> lk(connsMutex_);
    conns_.erase(id);
}

inline SendResult MultiReactorServer::sendTo(uint64_t connId,
                                             const void* data, size_t len) noexcept {
    if (len > MAX_FRAME_PAYLOAD) return SendResult::TooLarge;
    if (len && !data) return SendResult::InvalidArgument;
    if (!isRunning()) return SendResult::Closed;
    std::shared_lock<std::shared_mutex> submissions(submissionMutex_);
    if (!isRunning()) return SendResult::Closed;

    std::shared_ptr<Conn> c;
    {
        std::shared_lock<std::shared_mutex> lk(connsMutex_);
        auto it = conns_.find(connId);
        if (it == conns_.end()) return SendResult::NotConnected;
        c = it->second;
    }
    if (c->isClosed.load(std::memory_order_acquire)) return SendResult::Closed;

    std::vector<uint8_t> frame;
    try {
        frame.resize(FRAME_HEADER_SIZE + len);
        encodeFrameInto(frame.data(), FrameType::DATA, data, len);
    } catch (...) {
        return SendResult::Overloaded;
    }

    // Ceiling check on the credit above. See pendingBytes for why this being a
    // best-effort soft limit is what makes the relaxed orders there safe.
    size_t old = c->pendingBytes.load(std::memory_order_relaxed);
    for (;;) {
        if (old > cfg_.maxPendingBytes || frame.size() > cfg_.maxPendingBytes - old) {
            stats_.queueDrops.fetch_add(1, std::memory_order_relaxed);
            return SendResult::Overloaded;
        }
        if (c->pendingBytes.compare_exchange_weak(old, old + frame.size(),
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed)) break;
    }

    SubReactor* owner = c->owner;
    if (!owner || !owner->submitSend(connId, std::move(frame))) {
        c->pendingBytes.fetch_sub(FRAME_HEADER_SIZE + len, std::memory_order_relaxed);
        return SendResult::Overloaded;
    }
    return SendResult::Ok;
}

inline bool MultiReactorServer::closeConnection(uint64_t connId) noexcept {
    if (!isRunning()) return false;
    std::shared_lock<std::shared_mutex> submissions(submissionMutex_);
    if (!isRunning()) return false;
    std::shared_ptr<Conn> c;
    {
        std::shared_lock<std::shared_mutex> lk(connsMutex_);
        auto it = conns_.find(connId);
        if (it == conns_.end()) return false;
        c = it->second;
    }
    // Every exit from here reports itself in the same counter. The loss used to
    // be silent: nothing anywhere recorded that this connection was asked to go
    // away, so it simply waited for idle timeout while the application believed
    // it had hung up. (The name says "dropped" because that is what happens to
    // the request; the connection keeps living until something else ends it.)
    if (!c->owner) {
        stats_.closeRequestsDropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    if (!c->owner->submitClose(connId)) {
        stats_.closeRequestsDropped.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    return true;
}

inline size_t MultiReactorServer::connectionCount() const noexcept {
    std::shared_lock<std::shared_mutex> lk(connsMutex_);
    return conns_.size();
}

inline std::vector<uint64_t> MultiReactorServer::connectionIds() const {
    std::shared_lock<std::shared_mutex> lk(connsMutex_);
    std::vector<uint64_t> out;
    out.reserve(conns_.size());
    for (auto& kv : conns_) out.push_back(kv.first);
    return out;
}

}  // namespace industrial
