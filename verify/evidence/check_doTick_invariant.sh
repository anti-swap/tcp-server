#!/bin/bash
# Does doTick()'s sweep survive a broken invariant?
#
# doTick walks conns_ with a range-for and calls closeConn(), which erases from
# the same map whenever it tears down rather than parks. Its safety used to rest
# on "anything still in conns_ that has FIN'd is also marked closing" -- true,
# and unenforced. inject_fin_without_closing.py breaks it, which is what any
# refactor touching one of the five functions involved and not the others would
# do by accident.
#
# Both sides get the same injection, so the only difference is the doTick being
# tested. That is the point: proving the hazard is real (before: AddressSanitizer
# reports the use-after-free) and that the fix removes it rather than documents
# it (after: clean).
#
# Usage:
#   bash verify/evidence/check_doTick_invariant.sh
set -u
PROJ=${PROJ:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
cd "$PROJ" || exit 9
SRC=verify/evidence/repro_doTick_erase.cpp
INJ=verify/evidence/inject_fin_without_closing.py
FLAGS=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -g
       -fsanitize=address,undefined -fno-omit-frame-pointer)

rm -rf /tmp/dti_before /tmp/dti_after
mkdir -p /tmp/dti_before/verify /tmp/dti_after/verify

git show HEAD:tcp-server-mt.h > /tmp/dti_before/tcp-server-mt.h || exit 9
cp "$PROJ/tcp-server-mt.h"     /tmp/dti_after/tcp-server-mt.h

for label in before after; do
  python3 "$INJ" /tmp/dti_$label/tcp-server-mt.h /tmp/dti_$label/tcp-server-mt.h || exit 9
  cp "$SRC" /tmp/dti_$label/verify/ || exit 9
  ( cd /tmp/dti_$label && g++ "${FLAGS[@]}" verify/repro_doTick_erase.cpp \
      -o /tmp/dti_bin_$label ) || exit 9
done

declare -A verdict=()
for label in before after; do
  rm -f /tmp/dti_$label.out
  set +e
  timeout 120 /tmp/dti_bin_$label > /tmp/dti_$label.out 2>/tmp/dti_$label.err
  rc=$?
  set -e
  if grep -q "AddressSanitizer\|runtime error" /tmp/dti_$label.err; then
    crashed=yes
  else
    crashed=no
  fi
  echo "== $label rc=$rc crash_or_sanitizer_report=$crashed"
  tail -3 /tmp/dti_$label.out
  if [ "$crashed" = yes ]; then
    echo "   first report line:"
    grep -m1 "ERROR: AddressSanitizer\|runtime error" /tmp/dti_$label.err | sed 's/^/     /'
    grep -m1 -A3 "in SubReactor::doTick\|in doTick" /tmp/dti_$label.err | sed 's/^/     /'
  fi
  verdict[$label]=$crashed
done

echo
if [ "${verdict[before]}" = yes ] && [ "${verdict[after]}" = no ]; then
  echo "EXPECTED: the hazard fires before the fix and does not after"
  exit 0
fi
echo "UNEXPECTED: before=${verdict[before]} after=${verdict[after]}"
exit 1
