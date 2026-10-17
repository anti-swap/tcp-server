#!/usr/bin/env bash
# Is ThreadSanitizer lying about a condition variable here?
#
# verify/backpressure.cpp holds a worker inside a callback using the ordinary
# handshake -- unique_lock, wait_for with a predicate, the other side sets the
# flag under the lock and notifies. ThreadSanitizer reports "double lock of a
# mutex" at the setter followed by a data race on the flag "synchronized via
# sleep", while listing that very mutex as held by both sides.
#
# Either the handshake above is broken in a way three readings missed, or this
# build of TSan mis-models std::condition_variable::wait_for on this machine.
# Those two answers point in opposite directions, so this just asks: does the
# same report appear with no server, no sockets and no shared library besides
# libstdc++?
set -u
cd "$(mktemp -d)"

cat > cv.cpp <<'EOF'
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <thread>

std::mutex              m;
std::condition_variable cv;
bool                    released = false;
bool                    reached  = false;

int main() {
    bool raced = false;
    {
        std::thread t([&] {
            std::unique_lock<std::mutex> lk(m);
            reached = true;
            cv.wait_for(lk, std::chrono::seconds(20), [&] { return released; });
        });
        while (!reached) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        {
            std::lock_guard<std::mutex> lk(m);
            released = true;
        }
        cv.notify_all();
        t.join();
        raced = true;
    }
    std::printf("handshake completed: %s\n", raced ? "yes" : "no");
    return 0;
}
EOF

echo "== building the minimal handshake under ThreadSanitizer =="
g++ -std=c++17 -g -fsanitize=thread -pthread cv.cpp -o cv || exit 1

echo "== running =="
TSAN_OPTIONS=halt_on_error=0 ./cv > cv.out 2> cv.err
rc=$?
cat cv.out
if grep -q "ThreadSanitizer" cv.err; then
    echo
    echo "!! ThreadSanitizer complains about the bare handshake too:"
    grep -E "SUMMARY|WARNING" cv.err
    echo
    echo "=> the report is about this library/platform, not about the server."
else
    echo
    echo "no ThreadSanitizer output from the bare handshake."
    echo "=> whatever it saw in backpressure.cpp is real and still unexplained."
fi
exit 0
