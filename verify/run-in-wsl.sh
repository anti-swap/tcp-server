#!/usr/bin/env bash
# Run the tcp-server verification suite inside WSL (Linux).
#
# Executed from the Linux side. Windows paths are reached through drvfs, so the
# project does not have to be copied into the WSL filesystem first:
#
#     bash /mnt/c/Users/root/WorkBuddy/Claw/tcp-server/verify/run-in-wsl.sh
#
# Optional switches (each can be combined):
#   ENABLE_ASAN=1   rebuild under AddressSanitizer + UndefinedBehaviorSanitizer,
#                   which is what the lifetime and arithmetic defects need to be
#                   proven fixed rather than merely not observed.
#   ENABLE_TSAN=1   rebuild under ThreadSanitizer, which is what every published
#                   race in this file ultimately needed.
#
# Both are separate configurations rather than one combined build: ASan and TSan
# cannot be linked into the same binary, and a run under either is slow enough
# that folding it into the default pass would stop people running it.
#
# Not included here, and it cannot be: verify/build.sh type-checks the header
# against verify/stub/ with the native Windows toolchain. A Windows binary
# started from Linux gets no Windows environment, so its g++ fails to launch
# cc1plus at all. Run it from a Windows shell (bash verify/build.sh) -- it is a
# different compiler over a different surface, and it has already caught a class
# of defect this suite cannot see (a POSIX symbol the shim never declared).
#
# Idempotent: safe to rerun; the toolchain install is skipped once g++ exists.
set -euo pipefail

# Directory of this script -> project root (one level up from verify/).
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$here")"
cd "$proj"

echo "== project: $proj =="

# ---- toolchain ------------------------------------------------------------
if ! command -v g++ >/dev/null 2>&1; then
    echo "== installing build-essential (needs network + your sudo password) =="
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential
fi
g++ --version | head -1

# Every build below runs from the project root, so `include` resolves against
# it. Sources include the header by bare name; the ../.. arithmetic that used
# to sit in twenty files was the thing that made any move expensive.
base_flags=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -Iinclude)

# The test uses the same switches to build, so both must agree on the macro or
# the harness silently compiles itself away. It cannot: fault_injection.cpp
# #errors without it. That is deliberate -- see the file header.
build() {
    local label="$1"; shift
    echo "== building [$label] =="
    g++ "${base_flags[@]}" "$@" verify/close_semantics.cpp -o /tmp/close_semantics
    g++ "${base_flags[@]}" "$@" verify/idle_semantics.cpp -o /tmp/idle_semantics
    g++ "${base_flags[@]}" "$@" verify/ping_pong.cpp -o /tmp/ping_pong
    g++ "${base_flags[@]}" "$@" verify/send_results.cpp -o /tmp/send_results
    g++ "${base_flags[@]}" "$@" verify/backpressure.cpp -o /tmp/backpressure
    g++ "${base_flags[@]}" "$@" -DINDUSTRIAL_TCP_TESTING verify/fault_injection.cpp \
        -o /tmp/fault_injection
}

run_all() {
    local label="$1"
    local failed=0
    echo "---- running [$label] ----"

    for attempt in "close_semantics, default reactors:/tmp/close_semantics" \
                   "close_semantics, CS_REACTORS=1:env CS_REACTORS=1 /tmp/close_semantics" \
                   "idle_semantics:/tmp/idle_semantics" \
                   "ping_pong:/tmp/ping_pong" \
                   "send_results:/tmp/send_results" \
                   "backpressure:/tmp/backpressure" \
                   "fault_injection:/tmp/fault_injection"; do
        local name="${attempt%%:*}"
        local cmd="${attempt#*:}"
        local rc=0
        # A bounded run: a hang has to present as a failure, not as a long coffee.
        timeout 180 bash -c "$cmd" || rc=$?
        echo "   [$label] $name -> rc=$rc"
        [ "$rc" -eq 0 ] || failed=1
    done
    return "$failed"
}

overall=0

# ---- cpplint -------------------------------------------------------------
# Two things here are traps and both cost real time when missed.
#
# 1. cpplint exits 0 whether it found anything or not. Verified: a file holding
#    `int x = (size_t)y;` reports "Total errors found: 2" and still exits 0. So
#    the count has to be parsed out of its output; asking for $? is how this
#    becomes another check that cannot fail.
# 2. `Total errors found:` is printed only when the number is non-zero -- a clean
#    file prints nothing beyond "Done processing". Zero and "did not run" look
#    the same, so every run lints a file that certainly offends first, and the
#    clean result below is not believed unless that file did offend.
python3 -m cpplint --version >/dev/null 2>&1 || python3 -m pip install -q cpplint

run_cpplint() {
    echo "== cpplint =="
    local out control total
    printf 'int x = (size_t)y;\n' > .cpplint_control.h
    control=$(python3 -m cpplint --quiet .cpplint_control.h 2>&1 |
              sed -n 's/^Total errors found: //p')
    rm -f .cpplint_control.h
    case "$control" in
        ''|*[!0-9]*) echo "  [FAIL] the control run did not produce a finding count"
                     echo "         (got: '$control'); a clean result below would mean nothing"
                     return 1 ;;
    esac
    if [ "$control" -eq 0 ]; then
        echo "  [FAIL] cpplint reported 0 findings in a file that certainly has them"
        return 1
    fi
    echo "  control: $control findings, as expected"

    out=$(python3 -m cpplint --quiet include/tcp-server-mt.h 2>&1)
    case "$out" in
        *Traceback*|*"No such file"*|*"command not found"*)
            echo "  [FAIL] cpplint did not run:"
            printf '%s\n' "$out" | tail -5
            return 1 ;;
    esac
    total=$(printf '%s\n' "$out" | sed -n 's/^Total errors found: //p')
    [ -z "$total" ] && total=0     # printed only when the count is non-zero
    printf '  tcp-server-mt.h: %s findings\n' "$total"
    [ "$total" -eq 0 ]
}

run_cpplint || overall=1
echo
#
# Every verdict below reads one. Handing a command to WSL as an inline string
# (`bash -lc '...'`) reports $? = 0 for `false`, for `(exit 3)` and for a
# program compiled to return 7 -- the success path and the failure path become
# indistinguishable, and a suite whose outcome no longer depends on the outcome
# is decoration. Through a script file it travels correctly. This probe states
# which situation we are in before anything is trusted, rather than after.
if ! timeout 300 bash verify/probe_exit_code.sh; then
    echo "!! exit codes are not crossing into Linux correctly; results below are meaningless"
    exit 2
fi
echo

build "plain"
run_all "plain" || overall=1

if [ "${ENABLE_ASAN:-0}" = "1" ]; then
    build "asan+ubsan" -g -fsanitize=address,undefined -fno-omit-frame-pointer
    run_all "asan+ubsan" || overall=1
fi

if [ "${ENABLE_TSAN:-0}" = "1" ]; then
    build "tsan" -g -fsanitize=thread -fno-omit-frame-pointer
    run_all "tsan" || overall=1
fi

cat <<EOF

=================================================================
 Result: $([ "$overall" -eq 0 ] && echo "all configurations passed" || echo "FAILURES - see above")

 Reading the output:
  - close_semantics prints [phase] lines that dump every counter. Read them as
    deltas: accepted/rejected says whether the accept loop handed the connection
    over, queueDrops says whether a reactor refused the task, epollCtlErrors says
    whether it refused the descriptor, and reactorFailures says whether the
    worker is alive at all. From the client side all four look identical.
  - CS_REACTORS=1 matters because pickReactor() hands connections out
    round-robin: with several workers a fault that belongs to one of them gets
    averaged away.
  - idle_semantics asks what "idle" is measured in, and answers it with three
    peers at the same byte rate: one inside a frame it has not finished, one
    silent, one sending complete frames. Only the middle one may be closed. A
    failure in the first case with the other two still passing is the clock
    measuring frames instead of bytes.
  - ping_pong covers the read path's most conditional branch, which no case had
    executed at all: PING on a healthy connection, and PING in flight with a
    close. It asserts the PONG is counted (bytesSent == 8 per PONG, the header
    size) and that one past a close request is dropped rather than answered
    over the FIN.
  - send_results asks what sendTo() can say back: one assertion per SendResult
    value, because four of the five had none anywhere in the suite and a correct
    unguarded branch is free to change. Overloaded is reached by a frame larger
    than a 32-byte ceiling rather than by racing the drain. It also counts
    acceptErrors in a forked child whose descriptor ceiling is below the number
    of peers knocking -- the one accept failure that is predictable at startup
    and leaves no other symptom.
  - A [SKIP] line is a case that asked the machine a question first and did not
    like the answer. It names what it checked. It is not a pass. send_results'
    hup/err case is one of these on hosts that report an aborted connection as
    an ordinary FIN (WSL's 4.4 kernel does): EPOLLHUP|EPOLLERR is then never
    delivered, so the branch cannot be reached from there.
  - This run says nothing about verify/build.sh: that check is Windows-side only
    and has to be run separately, from a Windows shell.

 If a case fails, paste the whole output back -- first without CS_REACTORS, then
 with it.
=================================================================
EOF
exit "$overall"
