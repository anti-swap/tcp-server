#!/usr/bin/env bash
# Performance baseline for tcp-server-mt.h.
#
# Not part of CI, and the reason is not laziness: a shared runner gives its
# cores to other tenants, so a threshold there fails on someone else's load
# rather than on a regression. These numbers are worth having when they are
# compared against numbers taken on the same machine minutes earlier.
#
#   bash verify/bench.sh            # 5 repetitions, medians
#   REPS=3 bash verify/bench.sh     # quicker, noisier
#
# What it measures, and why each one:
#   rss     -- resident memory per idle connection. Buffers are allocated when a
#              connection is accepted, so this is the cost of `maxConnections`.
#   ingest  -- bytes per second a reactor can take off the socket and dispatch.
#              Run at 1, 2 and 4 reactors: the ratio is the only scaling claim
#              this project can currently make.
#   send    -- round trip through a handler that echoes with sendTo(), p50/p99.
#              p99 is the point: a mean hides the wakeup tail that a queue and
#              an eventfd hand-off produce.
#
# Every measurement runs REPS times and the median is printed. The first run of
# each binary is discarded: it pays for page faults and for the toolchain's
# lazy dynamic linking, neither of which belongs to the code being timed.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
proj="$(dirname "$here")"
cd "$proj"

REPS="${REPS:-5}"
flags=(-std=c++17 -O2 -pthread -Iinclude)
raw="$(mktemp)"
trap 'rm -f "$raw"' EXIT

echo "== building =="
g++ "${flags[@]}" verify/bench_rss.cpp  -o /tmp/bench_rss
g++ "${flags[@]}" verify/bench_rx.cpp   -o /tmp/bench_rx
g++ "${flags[@]}" verify/bench_send.cpp -o /tmp/bench_send
g++ --version | head -1
echo "   cores: $(nproc)"

echo "== running (${REPS} reps per measurement) =="
# One discarded run each, then the ones that count.
/tmp/bench_rss  1000 >/dev/null
/tmp/bench_rx   1 1 1 >/dev/null
/tmp/bench_send 200 56 >/dev/null

for _ in $(seq "$REPS"); do
    /tmp/bench_rss 1000 >>"$raw"
    /tmp/bench_rx 1 1 3 >>"$raw"
    /tmp/bench_rx 2 2 3 >>"$raw"
    /tmp/bench_rx 4 4 3 >>"$raw"
    /tmp/bench_send 2000 56 >>"$raw"
done

python3 - "$raw" <<'PY'
import statistics
import sys
from collections import defaultdict

groups = defaultdict(lambda: defaultdict(list))
for line in open(sys.argv[1], encoding="utf-8"):
    fields = line.split()
    if not fields:
        continue
    kind = fields[0]
    kv = dict(f.split("=", 1) for f in fields[1:] if "=" in f)
    key = (kind, tuple(sorted((k, v) for k, v in kv.items()
                              if k in ("connections", "reactors", "clients", "payload"))))
    for name, value in kv.items():
        if name in ("connections", "reactors", "clients", "payload"):
            continue
        try:
            groups[key][name].append(float(value))
        except ValueError:
            pass

print()
print("%-10s %-26s %s" % ("measure", "config", "median"))
print("-" * 62)
for (kind, cfg), metrics in groups.items():
    label = ", ".join("%s=%s" % kv for kv in cfg) if cfg else "-"
    for name, values in sorted(metrics.items()):
        print("%-10s %-26s %s = %8.2f  (min %.2f, max %.2f, n=%d)" % (
            kind, label, name, statistics.median(values),
            min(values), max(values), len(values)))
PY
