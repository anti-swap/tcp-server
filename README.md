# tcp-server

A Linux TCP server as a single header: C++17, level-triggered epoll, one accept
thread with N sub-reactors. Everything is in that one file.

```cpp
#include "tcp-server-mt.h"          // -I<repo>/include  -std=c++17  -pthread
industrial::MultiReactorServer server(cfg);
```

No `.cpp`, no build scripts, no third-party dependencies. To use it, copy
`include/tcp-server-mt.h` into your project. It needs Linux (`epoll` +
`eventfd`), so it cannot be linked into a running binary on the Windows machine
this was developed on -- hence the two kinds of checking described below.

## Shape of it

- **One accept thread plus N sub-reactors**, each reactor running on its own
  thread and owning its own connections. A submission aimed at another thread
  travels through that reactor's queue, plus an `eventfd` wakeup.
- **Closing is staged, not `close(fd)`.** A connection is marked `closing`
  first, drains the replies already queued, and only then FINs. That semantic is
  where eight rounds of review landed most often and where the probes are
  densest.
- **The counters live in `ServerStats`**, each annotated with when it moves and
  with the branches that should reach it but cannot. Reading those comments
  beats reading this summary.

## Verifying

This project has no "just run it and see" position. Two checks cover two halves:

```bash
# Windows/MSYS side: hand the header to a real compiler. 21 translation units,
# nothing linked, nothing executed.
bash verify/build.sh

# Linux/WSL side: actually run it. Seven suites, nine runs per configuration --
# two of them run twice, at different reactor counts -- over {plain, asan+ubsan,
# tsan}: 27 configurations, cpplint included.
wsl env ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run-in-wsl.sh
```

The environment variables have to go through `wsl env`. Writing
`ENABLE_ASAN=1 wsl ...` fails in silence and leaves you with output that looks
green having run plain only. That is a trap someone walked into, not a style
choice.

`build.sh` catches types, templates, `const`-correctness and every warning. It
cannot catch runtime behaviour at all. That is what the racing, sanitizer and
semantic cases inside `run-in-wsl.sh` are for. Neither half is optional.

CI runs the Linux half on every push and pull request -- see
`.github/workflows/c-cpp.yml`. It deliberately does not run `build.sh`, which
exists because this particular desk is not Linux, not because Linux lacks
anything.

`verify/bench.sh` is the third kind of check, and it is deliberately not in CI:
it measures resident memory per idle connection, ingest rate at one, two and
four reactors, and the echo round trip at p50 and p99. A shared runner hands its
cores to other tenants, so a threshold there would fail on someone else's load
rather than on a regression. The numbers are worth having when they are compared
against numbers taken on the same machine minutes earlier -- which is why none
of them are written down here. Each change that moves them records the before
and after in its commit message, where the date and the machine are attached.

## Layout

```
include/tcp-server-mt.h   the only deliverable, 2937 lines
verify/                   compile check + runtime suites + one-off evidence probes
docs/review/              eight rounds of review, with an index
CPPLINT.cfg               80 columns; nothing left in it but the copyright filter
```

Those twenty probes under `verify/evidence/` are not a standing test suite. Each
one was written to settle a single claim, against a history that has since been
fixed. Two of them read the header out of git at a given revision, so they accept
either layout -- before or after the directory move on 2026-10-18.

## Still open

Three rounds (the sixth, the seventh and the eighth) each closed carrying the
same few items they could not settle themselves. They were never disproved; they
were only never executed locally. Two of the three have since been closed:

- **`EPOLLHUP` / `EPOLLERR` -- closed, 2026-10-18, on a real kernel.** This case
  printed `[SKIP]` on every machine here, because those kernels hand an aborted
  connection over as an ordinary FIN and the events never arrive; asserting
  against a break the host cannot produce would be asserting against the host.
  CI put it somewhere that can: on the runner, `send_results` reports hup/err as
  having run and asserted under plain, asan+ubsan and tsan alike, with no other
  case in the suite declining. The verdict comes back as an annotation filed by
  the step named "what this kernel was willing to tell us", not as an exit code,
  because a `[SKIP]` leaves the exit code alone: nothing else in the output
  separates "asserted and passed" from "never ran".

- **Which reactor a connection lands on -- closed, 2026-10-18.** The note here
  used to say that reactor-count-dependent behaviour had only been run with
  `numReactors = 1`, which was no longer true and had not been for some rounds:
  `close_semantics` has defaulted to two since the seventh, and the runner also
  takes it down to one on purpose, because with a single worker a fault that
  belongs to one worker is averaged away and reads as a pass. What was really
  unasserted was narrower and worse -- nothing observed `pickReactor()`'s
  round-robin, so returning the first reactor every time left all 21
  configurations green. `verify/reactor_spread.cpp` asks it directly: eight
  connections over four reactors have to be handled by four distinct threads. It
  runs at four reactors and again at two, so the assertion is shown to follow the
  configuration rather than a constant, and both existing two-reactor suites were
  run against a deliberately broken `pickReactor()` first to confirm they could
  not have caught it.

One remains open:

- **One count folded into another**: a certain class of residual drops now lands
  in `queueDrops` / `bytesDropped`, counted the same as a genuine close branch.
  Under the current contract that is the same event; separating them would take a
  new counter.

## Review

This header went through five full review rounds, plus three spent verifying
claims raised from outside. Every defect had a probe written to make it fail
first, then a fix, then that same probe run green again. The records and their
index are in [`docs/review/README.md`](docs/review/README.md).

## License

Apache License 2.0. See [LICENSE](LICENSE).
