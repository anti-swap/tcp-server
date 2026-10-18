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
#   bash verify/evidence/check_doTick_invariant.sh [ref-for-before]
#
# Pass the revision to test against once the fix is in: left to default to HEAD
# it would compare the fix with itself. The commit whose doTick still closed
# in-loop is 1f4f899^ HEAD-side, i.e. any tree before f27cfc1.
set -u
PROJ=${PROJ:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}
BEFORE_REF=${1:-HEAD}
cd "$PROJ" || exit 9
SRC=verify/evidence/repro_doTick_erase.cpp
INJ=verify/evidence/inject_fin_without_closing.py
FLAGS=(-std=c++17 -Wall -Wextra -Wpedantic -Wshadow -pthread -g
       -fsanitize=address,undefined -fno-omit-frame-pointer)

rm -rf /tmp/dti_before /tmp/dti_after
mkdir -p /tmp/dti_before/verify/evidence /tmp/dti_after/verify/evidence

# The header moved to include/ on 2026-10-18. Every rev before that still has it
# at the repository root, and `git show` to a path a rev does not carry simply
# fails -- so ask which one exists instead of assuming. A probe that only works
# on half the history is a probe that quietly stops being evidence.
hdr_in() {
  for p in include/tcp-server-mt.h tcp-server-mt.h; do
    if git cat-file -e "$1:$p" 2>/dev/null; then printf '%s' "$p"; return 0; fi
  done
  echo "check_doTick_invariant: no tcp-server-mt.h in $1" >&2
  return 1
}
HDR_BEFORE=$(hdr_in "$BEFORE_REF") || exit 9
HDR_AFTER=include/tcp-server-mt.h
[ -f "$HDR_AFTER" ] || HDR_AFTER=tcp-server-mt.h

git show "$BEFORE_REF:$HDR_BEFORE" > /tmp/dti_before/tcp-server-mt.h || exit 9
cp "$PROJ/$HDR_AFTER"              /tmp/dti_after/tcp-server-mt.h

for label in before after; do
  python3 "$INJ" /tmp/dti_$label/tcp-server-mt.h /tmp/dti_$label/tcp-server-mt.h || exit 9
  mkdir -p /tmp/dti_$label/verify/evidence
  cp "$SRC" /tmp/dti_$label/verify/evidence/ || exit 9
  # -I. matters now: the sources include the header by bare name, and the copy
  # under test sits at the root of this throwaway tree, not beside the .cpp.
  ( cd /tmp/dti_$label && g++ "${FLAGS[@]}" -I. verify/evidence/repro_doTick_erase.cpp \
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
