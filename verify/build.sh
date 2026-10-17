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
# Usage:  bash verify/build.sh
# Exit:   0 = clean, 1 = diagnostics found, 2 = toolchain missing

set -u

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(dirname "$here")"

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

echo "== compiling $(basename "$root")/tcp-server-mt.h =="
if ! "$CXX" "${flags[@]}" "$HERE_W/compile_check.cpp" -o "$HERE_W/compile_check.o" 2>&1; then
  echo "build.sh: FAILED" >&2
  rm -f "$here/compile_check.o"
  exit 1
fi
rm -f "$here/compile_check.o"

# The runtime test is compiled too (never linked or run here): it includes the
# header from a second translation unit and exercises APIs the trivial checker
# does not, so it catches interface drift -- e.g. a renamed public method -- that
# compile_check.cpp would happily keep compiling.
if [ -f "$here/close_semantics.cpp" ]; then
  echo "== compiling $(basename "$root")/verify/close_semantics.cpp =="
  if ! "$CXX" "${flags[@]}" "$HERE_W/close_semantics.cpp" -o "$HERE_W/close_semantics.o" 2>&1; then
    echo "build.sh: FAILED" >&2
    rm -f "$here/close_semantics.o"
    exit 1
  fi
  rm -f "$here/close_semantics.o"
fi

echo "build.sh: OK (no warnings)"
