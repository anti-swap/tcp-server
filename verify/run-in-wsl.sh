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

# A combine the switches the way every run needs them, plus the ones requested.
base_flags=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread)

# The test uses the same switches to build, so both must agree on the macro or
# the harness silently compiles itself away. It cannot: fault_injection.cpp
# #errors without it. That is deliberate -- see the file header.
build() {
    local label="$1"; shift
    echo "== building [$label] =="
    g++ "${base_flags[@]}" "$@" verify/close_semantics.cpp -o /tmp/close_semantics
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

    out=$(python3 -m cpplint --quiet tcp-server-mt.h 2>&1)
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
  - A [SKIP] line is a case that asked the machine a question first and did not
    like the answer. It names what it checked. It is not a pass.

 If a case fails, paste the whole output back -- first without CS_REACTORS, then
 with it.
=================================================================
EOF
exit "$overall"
