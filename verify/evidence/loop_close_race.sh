#!/bin/bash
# Case F (a close racing the reply it produced) is a probabilistic detector:
# the window is microseconds wide and one run of the test tells you almost
# nothing. This drives N runs against two builds and counts how many of them
# reproduce, which is the only comparison worth reading.
#
#   before = the tree at a given revision (defaults to HEAD)
#   after  = the working tree
#
# Usage:
#   bash verify/evidence/loop_close_race.sh [N] [ref-for-before]
#
# Measured on this machine, 20 rounds per run, ThreadSanitizer, CS_REACTORS=1:
#   before the drainInputThenClose fix: 4 of 8 runs reproduced
#   after:                              0 of 8 runs reproduced
set -u
PROJ=${PROJ:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
N=${1:-8}
BEFORE_REF=${2:-HEAD}
cd "$PROJ" || exit 9

rm -rf /tmp/cfr_before /tmp/cfr_after
mkdir -p /tmp/cfr_before/verify /tmp/cfr_after/verify

git show "$BEFORE_REF":tcp-server-mt.h > /tmp/cfr_before/tcp-server-mt.h || exit 9
git show "$BEFORE_REF":verify/close_semantics.cpp > /tmp/cfr_before/verify/close_semantics.cpp || exit 9
cp "$PROJ/tcp-server-mt.h"                 /tmp/cfr_after/tcp-server-mt.h
cp "$PROJ/verify/close_semantics.cpp"      /tmp/cfr_after/verify/close_semantics.cpp

FLAGS=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -g
       -fsanitize=thread -fno-omit-frame-pointer)

for label in before after; do
  build_args=("${FLAGS[@]}")
  src_dir=/tmp/cfr_$label
  # Deliberately unpatched: adding fprintf inside the close path changes the
  # timing of the very window being measured, which would make the two sides
  # incomparable. Tracing is a separate step -- see trace_close_race.py.
  ( cd "$src_dir" && g++ "${build_args[@]}" verify/close_semantics.cpp \
      -o /tmp/cfr_bin_$label ) || exit 9
  repro=0
  clean=0
  for i in $(seq 1 "$N"); do
    rm -f /tmp/cfr_run.out
    CS_REACTORS=1 timeout 300 /tmp/cfr_bin_$label > /tmp/cfr_run.out 2>/tmp/cfr_run.err
    rc=$?
    line=$(grep "of 20 rounds lost" /tmp/cfr_run.out || echo "no result line")
    echo "$label run $i rc=$rc :: $line"
    case "$line" in
      "      0 of 20 rounds lost a reply they had produced") clean=$((clean+1)) ;;
      *) repro=$((repro+1)) ;;
    esac
  done
  echo "== $label: clean=$clean repro_or_broken=$repro (of $N)"
  echo "   last trace retained at /tmp/cfr_run.err"
done
