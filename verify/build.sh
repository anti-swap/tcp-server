#!/usr/bin/env bash
# Host-side compile check for tcp-server-mt.h.
#
# The header is Linux-only (epoll + eventfd), so a native Linux toolchain is the
# only thing that can produce a real binary. This script exists to catch what a
# Linux box is expensive for: type errors, const-correctness, template mistakes
# and warnings -- on every edit, in seconds.
#
# It compiles against verify/stub/, a declaration-only POSIX surface. Nothing is
# linked. A clean run here means "the header is well-formed C++17"; it does NOT
# mean "it runs", and it cannot validate runtime behaviour.
#
# Every translation unit under verify/ is compiled, not just the trivial
# checker. Coverage is the whole point: a shim gap in a file nobody type-checks
# is still a gap, and it decides whether the next one is found now or months
# later. Compiling two files here while the header had already grown symbols the
# shim never declared (RLIM_INFINITY, setrlimit, accept, SO_RCVBUF,
# MSG_DONTWAIT, usleep) is how this script came to be failing on a tree that
# every Linux-side check called green.
#
# Usage:  bash verify/build.sh
# Exit:   0 = clean, 1 = diagnostics found, 2 = toolchain missing

set -u

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$here")"

# This script runs on the Windows side only -- from MSYS/Git Bash, not from WSL.
# It is deliberately not part of verify/run-in-wsl.sh: the compiler here is a
# Windows binary, and launching one from Linux leaves it without a Windows
# environment, so g++ dies with "cannot execute 'cc1plus'" (CreateProcess) rather
# than compiling anything. Anything that needs this check has to come back to a
# Windows shell.
#
# g++ is a native Windows binary: it cannot resolve MSYS-style /c/... paths.
to_win() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi; }
HERE_W="$(to_win "$here")"

# cc1plus needs its own DLLs on PATH; invoking the compiler by absolute path
# alone makes it fail silently with exit 1 and no diagnostics.
CXX="${CXX:-/c/msys64/ucrt64/bin/g++.exe}"
export PATH="/c/msys64/ucrt64/bin:${PATH}"

if [ ! -x "$CXX" ]; then
  echo "build.sh: no C++ compiler at $CXX (set CXX=...)" >&2
  exit 2
fi

flags=(-std=c++17 -c -Wall -Wextra -Wpedantic -Wshadow -D__linux__ -I "$HERE_W/stub")

# One TU. Extra flags are per-file rather than per-script: fault_injection.cpp
# refuses to compile without the macro that opens the header's test switches,
# which is the same macro the Linux runner passes it.
compile_tu() {
  local rel="$1"
  local extra=()
  case "$rel" in
    *fault_injection*) extra=(-DINDUSTRIAL_TCP_TESTING) ;;
  esac

  local stem
  stem="$(basename "$rel" .cpp)"
  local obj="$here/$stem.o"

  echo "== compiling verify/${rel} =="
  if ! "$CXX" "${flags[@]}" ${extra[@]+"${extra[@]}"} \
       "$(to_win "$here/$rel")" -o "$(to_win "$obj")" 2>&1; then
    echo "build.sh: FAILED on verify/${rel}" >&2
    rm -f "$obj"
    return 1
  fi
  rm -f "$obj"
  return 0
}

tus=()
for f in "$here"/*.cpp; do
  [ -e "$f" ] && tus+=("$(basename "$f")")
done
# The evidence probes are compiled too, never run here: each one encodes a
# specific claim about the header, and a header change that breaks one silently
# -- by making it no longer compile -- is exactly the drift this catches.
for f in "$here"/evidence/*.cpp; do
  [ -e "$f" ] && tus+=("evidence/$(basename "$f")")
done

if [ "${#tus[@]}" -eq 0 ]; then
  echo "build.sh: no translation units found under $here" >&2
  exit 1
fi

for tu in "${tus[@]}"; do
  compile_tu "$tu" || exit 1
done

echo "build.sh: OK (no warnings) -- ${#tus[@]} translation units"
