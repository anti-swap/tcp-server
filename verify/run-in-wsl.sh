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
echo "== running =="
/tmp/close_semantics
rc=$?

cat <<EOF

Exit code: $rc
 - 0 = every case passed
 - non-zero = see the FAIL lines above; each one names the assertion that broke

If a case fails, paste the whole output back -- the per-case line tells us
whether it was the FIN/RST distinction (P1-4) or the accounting.
EOF
exit "$rc"
