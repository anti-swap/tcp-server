#!/usr/bin/env bash
# Run the tcp-server close-semantics test inside WSL Ubuntu.
#
# Executed from the Ubuntu side. Windows paths are reached through drvfs, so the
# project does not have to be copied into the WSL filesystem first:
#
#     bash /mnt/c/Users/root/WorkBuddy/Claw/tcp-server/verify/run-in-wsl.sh
#
# Optional: ENABLE_ASAN=1 to additionally build/run under AddressSanitizer +
# UndefinedBehaviorSanitizer, which is what the lifetime defects (P0-1/P0-2)
# actually need to be proven fixed.
#
# Idempotent: safe to rerun; the toolchain install is skipped once g++ exists.
set -euo pipefail

# Directory of this script -> project root (one level up from verify/).
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$here")"
cd "$proj"

echo "== project: $proj =="

# Cleanup probe left behind by an earlier exploration attempt.
rm -f /tmp/.wsl_probe

# ---- toolchain ------------------------------------------------------------
if ! command -v g++ >/dev/null 2>&1; then
    echo "== installing build-essential (needs network + your sudo password) =="
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential
fi
g++ --version | head -1

# ---- build ----------------------------------------------------------------
echo "== compiling verify/close_semantics.cpp =="
CXXFLAGS=(-std=c++17 -Wall -Wextra -Wpedantic -pthread)
if [ "${ENABLE_ASAN:-0}" = "1" ]; then
    CXXFLAGS+=(-g -fsanitize=address,undefined -fno-omit-frame-pointer)
fi
g++ "${CXXFLAGS[@]}" verify/close_semantics.cpp -o /tmp/close_semantics

# ---- run ------------------------------------------------------------------
# Both reactor counts: pickReactor() hands connections out round-robin, so with
# more than one worker a fault that belongs to a single reactor gets averaged
# away. CS_REACTORS=1 puts every client on one worker and makes that visible.
run_once() {
    local rc=0
    "$@" || rc=$?      # no set -e here: the exit code is the result, not a trap
    return "$rc"
}

echo "== running (default: 2 reactors) =="
run_once /tmp/close_semantics
rc_default=$?

echo
echo "== running again with CS_REACTORS=1 =="
run_once env CS_REACTORS=1 /tmp/close_semantics
rc_single=$?

cat <<EOF

Exit codes: default=$rc_default  CS_REACTORS=1=$rc_single
 - 0 = every case passed
 - non-zero = see the FAIL lines above; each one names the assertion that broke

The [phase] lines dump every counter. Read them as deltas between phases:
accepted/rejected tells whether the accept loop handed the connection over,
queueDrops tells whether a reactor refused the task, epollCtlErrors tells
whether it refused the descriptor, and reactorFailures tells whether the
worker is still alive at all. From the client side all four look identical.

If a case fails, paste the whole output back -- first without CS_REACTORS, then
with it.
EOF
if [ "$rc_default" -ne 0 ] || [ "$rc_single" -ne 0 ]; then exit 1; fi
exit 0
