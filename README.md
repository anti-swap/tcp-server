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

# Linux/WSL side: actually run it. 7 suites x {plain, asan+ubsan, tsan} = 21
# configurations, cpplint included.
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
were only never executed locally. The first of them now has been:

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

Two remain open:

- **Nothing asserts which reactor a connection lands on**: behaviour with
  several reactors *is* measured -- `close_semantics` has defaulted to two since
  the seventh round and the runner also takes it down to one on purpose, and
  `fault_injection` and `compile_check` run at two -- but the round-robin itself
  is observed by nothing. An implementation that always returned the first
  reactor would leave every configuration green. What the single-reactor suites
  (`send_results`, `backpressure`, `ping_pong`, `idle_semantics`) do not gain
  from being run at N is the cross-thread path: the test thread is never a
  reactor thread, so submission through the per-reactor queue and its eventfd is
  exercised at any reactor count.
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
