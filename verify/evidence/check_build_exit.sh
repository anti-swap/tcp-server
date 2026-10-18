#!/usr/bin/env bash
# Exit-code sentinel for verify/build.sh.
#
# build.sh prints "FAILED" on stderr but the interesting half of its contract is
# the exit status, and a pipeline (`bash verify/build.sh | head`) reports the
# status of the last command instead. This runs it standalone and re-exits with
# whatever it returned, so "build.sh said OK" and "build.sh exited 0" can never
# drift apart unnoticed.
#
# Usage:  bash verify/evidence/check_build_exit.sh
# Exit:   whatever build.sh exited with.

set -u

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$(dirname "$here")")"

cd "$root"
bash verify/build.sh
rc=$?
echo "check_build_exit: build.sh exit=$rc"
exit "$rc"
