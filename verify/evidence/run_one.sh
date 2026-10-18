#!/usr/bin/env bash
# Run one verify/ case under Linux and report its real exit status.
#
# This exists because of a trap that already cost a verdict in this project:
# handing a command to WSL inline (`bash -lc '...; echo $?'`) lets the outer
# shell expand $? first, so a binary compiled to return 1 reports 0 and a red
# run looks green. The status has to be observed by a script file that travelled
# to disk, which is what this is.
#
# Usage: bash verify/evidence/run_one.sh <source-under-verify/> [args...]
set -uo pipefail

src="$1"; shift
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$(dirname "$here")")"
cd "$proj"

out="/tmp/$(basename "$src" .cpp)"
flags=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -O1)
case "$src" in
    *fault_injection*) flags+=(-DINDUSTRIAL_TCP_TESTING) ;;
esac

g++ "${flags[@]}" "$src" -o "$out" || { echo "build failed"; exit 2; }
"$out" "$@"
rc=$?
echo "rc=$rc"
exit "$rc"
