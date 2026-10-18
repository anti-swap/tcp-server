// verify/reactor_spread.cpp
//
// What this covers that nothing else did: pickReactor() hands accepted
// connections out round-robin, and until this file existed no case in the tree
// asked which reactor owns a connection. Every other suite either pins
// numReactors = 1 or, like close_semantics, uses two and exercises both -- but
// "both workers behave" and "the connections are actually spread over them" are
// different claims, and only the first one had a test. An implementation that
// always returned reactors_[0] left all 21 configurations green.
//
// The owner needs no new API to observe. A reactor runs its handlers on its own
// thread, so the thread that calls the message handler is the owner. Eight
// connections over four reactors must therefore come back as exactly four
// distinct threads, and closing all eight from the main thread -- which owns
// none of them -- walks the cross-reactor submission path once per connection.
//
// What is asserted:
//   1. Accepted connections are spread over min(reactors, connections) threads.
//   2. Closing every connection from a thread that owns none of them is
//      accepted, completes, and is counted -- with no reactor failure, no
//      dropped close request and no dropped queued task.
//   3. A stopped server reports no port and no connections.
//
// RS_REACTORS overrides the count so the assertion can be shown to follow the
// configuration rather than a constant: the runner takes it down to two.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread -Iinclude verify/reactor_spread.cpp

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
#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;
using industrial::ServerStats;

namespace {

int gFailures = 0;

void expect(bool ok, const char* what) {
    std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what);
    if (!ok) ++gFailures;
}

bool waitFor(int ms, const std::function<bool()>& done) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (done()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return done();
}

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

bool sendAll(int fd, const void* p, size_t n) {
    const uint8_t* b = static_cast<const uint8_t*>(p);
    size_t off = 0;
    while (off < n) {
        const ssize_t k = ::send(fd, b + off, n - off, 0);
        if (k <= 0) {
            if (k < 0 && errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(k);
    }
    return true;
}

std::vector<uint8_t> frame(FrameType t, const void* p, size_t n) {
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + n);
    industrial::encodeFrameInto(f.data(), t, p, n);
    return f;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    const int kConnections = 8;
    int reactors = 4;
    if (const char* r = std::getenv("RS_REACTORS")) reactors = std::atoi(r);
    if (reactors < 1) reactors = 1;

    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = 0;        // let the kernel choose; port() reports it
    cfg.numReactors   = reactors;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 60000;    // nothing here may end by a timer
    cfg.closeLingerMs = 300;

    std::printf("== %d connections over %d reactor(s) ==\n",
                kConnections, reactors);

    MultiReactorServer server(cfg);

    std::mutex ownersMutex;
    std::map<uint64_t, std::thread::id> owners;   // connection -> its reactor
    std::atomic<int> framesHandled{0};
    std::atomic<int> closeCallbacks{0};

    server.setMessageHandler([&](uint64_t id, const uint8_t*, size_t) {
        std::lock_guard<std::mutex> lock(ownersMutex);
        owners[id] = std::this_thread::get_id();
        framesHandled.fetch_add(1);
    });
    server.setCloseHandler([&](uint64_t, const std::string&, const char*) {
        closeCallbacks.fetch_add(1);
    });

    if (!server.start()) {
        expect(false, "start() ok");
        return 1;
    }

    // Port 0 means the kernel picked, so this is the only place the number
    // exists. A server that reports 0 here has connections going nowhere.
    const uint16_t port = server.port();
    expect(port != 0, "port() reports the port the kernel chose");

    std::vector<int> fds;
    for (int i = 0; i < kConnections; ++i) {
        const int fd = connectTo(port);
        if (fd < 0) break;
        fds.push_back(fd);
    }
    expect(static_cast<int>(fds.size()) == kConnections, "all clients connected");
    if (static_cast<int>(fds.size()) != kConnections) {
        for (int fd : fds) ::close(fd);
        server.stop();
        return 1;
    }

    // One frame each: it registers each connection with a reactor and makes
    // that reactor run the handler, which is where the owner becomes visible.
    const std::vector<uint8_t> request = frame(FrameType::DATA, "hi", 2);
    int sent = 0;
    for (int fd : fds) {
        if (sendAll(fd, request.data(), request.size())) ++sent;
    }
    expect(sent == kConnections, "one frame sent on every connection");

    const int expectedOwners = std::min(reactors, kConnections);
    waitFor(5000, [&] { return framesHandled.load() == kConnections; });
    expect(framesHandled.load() == kConnections,
           "every frame reached a handler");

    std::map<std::thread::id, int> perThread;
    {
        std::lock_guard<std::mutex> lock(ownersMutex);
        for (const auto& kv : owners) ++perThread[kv.second];
    }
    std::printf("      %zu connection(s) seen from %zu thread(s), expected %d:"
                "\n", owners.size(), perThread.size(), expectedOwners);
    for (const auto& kv : perThread) {
        std::printf("        thread %08zx handled %d\n",
                    std::hash<std::thread::id>{}(kv.first) & 0xffffffffu,
                    kv.second);
    }
    // The distribution itself, not the balance: round-robin over an atomic
    // counter owes every connection the next reactor in turn, but the count
    // per thread is not asserted -- a rejected connection advances the counter
    // without consuming a slot, and a test that forbids that would be
    // forbidding a documented behaviour.
    expect(static_cast<int>(perThread.size()) == expectedOwners,
           "connections are spread over min(reactors, connections) threads");

    // Close every connection from the main thread, which owns none of them.
    // Each of these is therefore a cross-reactor submission, one per
    // connection, and a queue that silently drops them shows up as a
    // connection that never closes rather than as a crash.
    const std::vector<uint64_t> ids = server.connectionIds();
    expect(static_cast<int>(ids.size()) == kConnections,
           "the registry holds every connection");
    int accepted = 0;
    for (uint64_t id : ids) {
        if (server.closeConnection(id)) ++accepted;
    }
    expect(accepted == kConnections, "every close request was accepted");

    // Our end has to go too, or each close waits out closeLingerMs.
    for (int fd : fds) ::close(fd);

    expect(waitFor(5000, [&] { return server.connectionCount() == 0; }),
           "every connection closed");
    waitFor(2000, [&] { return closeCallbacks.load() == kConnections; });

    const ServerStats& s = server.stats();
    expect(s.accepted.load() == static_cast<uint64_t>(kConnections),
           "accepted counts each connection once");
    expect(s.rejected.load() == 0, "nothing was rejected");
    expect(s.reactorFailures.load() == 0, "no reactor failed");
    expect(s.closeRequestsDropped.load() == 0, "no close request was dropped");
    expect(s.queueDrops.load() == 0, "no queued task was dropped");
    expect(closeCallbacks.load() == kConnections,
           "every close ran its callback exactly once");

    server.stop();
    expect(server.port() == 0, "a stopped server reports no port");
    expect(server.connectionCount() == 0,
           "a stopped server reports no connections");

    std::printf("\n%s (%d failure%s)\n", gFailures == 0 ? "ALL PASS" : "FAILURES",
                gFailures, gFailures == 1 ? "" : "s");
    return gFailures == 0 ? 0 : 1;
}
