# Review record

Eight rounds of code review against one header, `include/tcp-server-mt.h`, in
the order they happened.

The files were named one at a time as they were written on 2026-10-17 and
2026-10-18, which left three conventions behind: the first round carries no date
(`CODE-REVIEW.md`), the second carries a date but no number
(`CODE-REVIEW-2026-10-18.md`), and the number only appears from the third round
on. **The names were never unified** -- they cite each other, and each is a
snapshot of what was true when it was written; see the three cautions below. An
index table absorbs the mess more cheaply than rewriting eight historical
documents.

## Contents

| # | File | Date | Kind | Main entries |
|---|---|---|---|---|
| 1 | [CODE-REVIEW.md](CODE-REVIEW.md) | 2026-10-17 | full static review | The first pass; defects P0-1 to P2-6, each later fixed and verified on Linux |
| 2 | [CODE-REVIEW-2026-10-18.md](CODE-REVIEW-2026-10-18.md) | 2026-10-18 | full re-review | Confirmed every round-1 item fixed, then rescanned to report only what survived or was new |
| 3 | [CODE-REVIEW-2026-10-18-round3.md](CODE-REVIEW-2026-10-18-round3.md) | 2026-10-18 | full re-review | P1: `framesSent` counted frames that were never sent as sent, permanently. Also contains an inference this round retracted on purpose |
| 4 | [CODE-REVIEW-2026-10-18-round4.md](CODE-REVIEW-2026-10-18-round4.md) | 2026-10-18 | one item from outside | Whether `finishPeerClosed()`'s park branch busy-loops after a failed `epoll_ctl(MOD)` |
| 5 | [CODE-REVIEW-2026-10-18-round5.md](CODE-REVIEW-2026-10-18-round5.md) | 2026-10-18 | full re-review | R5-1 to R5-7. P1: zero coverage of PING/PONG and idle semantics. Two P2s: idleness measured only in whole frames, and `teardownClosed()` doing four throwing `push_back`s inside `noexcept` |
| 6 | [CODE-REVIEW-2026-10-18-round6.md](CODE-REVIEW-2026-10-18-round6.md) | 2026-10-18 | full re-review | R6-1 to R6-7. P1: dropping the last owner inside a close callback makes the process abort, always. Three more about what gets counted how |
| 7 | [CODE-REVIEW-2026-10-18-round7.md](CODE-REVIEW-2026-10-18-round7.md) | 2026-10-18 | one item from outside | When `close` and the peer's FIN land in one `handleRead`, the reply just queued is swallowed by an early FIN |
| 8 | [CODE-REVIEW-2026-10-18-round8.md](CODE-REVIEW-2026-10-18-round8.md) | 2026-10-18 | four items from outside | The close budget denominated in frames rather than bytes; one connection counted in both `accepted` and `rejected` when `stateHandler` throws; `handleWrite` beating `processPending`; `unregisterConn` clearing `closing` while `cleanup` never rolled back |

Rounds 4, 7 and 8 are all "someone raised it, this side checked it" -- not full
reviews, and much shorter accordingly. For what a full pass produces, read rounds
1, 2, 3, 5 and 6.

## Three things to know before reading them

**One, the path has moved.** Every report calls the header `tcp-server-mt.h`,
because they were written before the directory move on 2026-10-18, when it
really was at the repository root. It is now `include/tcp-server-mt.h`.

**Two, the line numbers have drifted.** A reference like `tcp-server-mt.h:880`
holds only against the baseline it was written from. The header grew from 2032
lines to its present 2937, and a reflow to 80 columns plus translating the
comments into English moved most lines in between. The baseline named at the top
of each report is the tree its line numbers refer to.

**Three, most of the commit hashes quoted inside them no longer resolve.** On
2026-10-18 the whole history was replayed onto the remote `Initial commit`, to
land on top of the LICENSE GitHub generated in the web UI, and all 97 commits
were rewritten. The old hashes resolve today only because the objects are still
held in this clone's `.git` by its reflog; one `git gc` and they are gone. Editing
them out of eight documents was not worth it, so here is the table instead. Each
pair was checked tree against tree -- the only difference is the
`.gitignore` / `LICENSE` / `README.md` that `Initial commit` brought:

| Hash in the reports | Hash now | Header lines at the time |
|---|---|---|
| `28a1615` | `d299e07` | 2032 |
| `0c9d363` | `0913c11` | 2152 |
| `7454530` | `39f74b3` | 2358 |
| `93e1673` | `e61c1c3` | 2436 |

## The rule every round followed

Nothing was written down as a defect without runtime evidence. Each one had a
probe written first to make it actually fail, then a fix, then the same probe run
again to watch it go green -- and the fix and the proof went in as two separate
commits. That held from round 1 to round 8, and it is the part of these reports
worth reading more than the conclusions themselves.
