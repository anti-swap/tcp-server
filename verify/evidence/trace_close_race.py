#!/usr/bin/env python3
# Patches a throwaway copy of the header with decision traces, so the ordering
# inside a lost close can be read off a log instead of guessed at.
#
# This is how case F (20 rounds of a close racing its own reply) was diagnosed.
# The window is microseconds wide and single observations are misleading: the
# first three hypotheses were all plausible and all wrong. The trace printed the
# answer in one line per decision:
#
#   [t] SUBMIT_SEND id=22      <- handler queued the first answer
#   [t] SUBMIT_CLOSE id=22     <- the application's close lands between them
#   [t] SUBMIT_SEND id=22      <- handler queued the second answer
#   [t] TASK_SEND id=22        <- first answer runs, accepted
#   [t] PARK_IMMEDIATE id=22   <- close task sees nothing to read, parks now
#   [t] TASK_SEND id=22        <- second answer runs against a closing conn
#   [t] REFUSE id=22
#
# Usage:
#   python3 verify/evidence/trace_close_race.py <header-in> <header-out>
#
# Then build the traced copy against close_semantics.cpp. The test includes the
# header by bare name now, so the build carries -I<dir> and the layout is
# <dir>/tcp-server-mt.h plus <dir>/verify/close_semantics.cpp. Until 2026-10-18
# the include was "../tcp-server-mt.h", relying on the directory shape alone --
# see loop_close_race.sh.
import sys

PATCHES = [
    # Which branch of drainInputThenClose parked, and why
    (
        '    if (served == 0 || c->waitingOnReplies) {\n'
        '        parkClose(c, "requested");',
        '    if (served == 0 || c->waitingOnReplies) {\n'
        '        std::fprintf(stderr, "[t] PARK_IMMEDIATE id=%llu served=%d wor=%d\\n",\n'
        '                    (unsigned long long)c->id, served, (int)c->waitingOnReplies);\n'
        '        parkClose(c, "requested");',
    ),
    # A close that decided to take one more queue round
    (
        '    c->waitingOnReplies = true;\n'
        '    const uint64_t id = c->id;\n'
        '    if (enqueue([this, id]() noexcept {',
        '    std::fprintf(stderr, "[t] PARK_DEFER id=%llu served=%d\\n",\n'
        '                (unsigned long long)c->id, served);\n'
        '    c->waitingOnReplies = true;\n'
        '    const uint64_t id = c->id;\n'
        '    if (enqueue([this, id]() noexcept {',
    ),
    # Submission of a reply, before it becomes a task
    (
        'inline bool SubReactor::submitSend(uint64_t id,\n'
        '                                   std::vector<uint8_t> frame) noexcept {\n'
        '    std::function<void()> task;',
        'inline bool SubReactor::submitSend(uint64_t id,\n'
        '                                   std::vector<uint8_t> frame) noexcept {\n'
        '    std::fprintf(stderr, "[t] SUBMIT_SEND id=%llu\\n", (unsigned long long)id);\n'
        '    std::function<void()> task;',
    ),
    # The reply task actually running
    (
        '        task = [this, id, f = std::move(frame)]() noexcept {\n'
        '            auto it = conns_.find(id);',
        '        task = [this, id, f = std::move(frame)]() noexcept {\n'
        '            std::fprintf(stderr, "[t] TASK_SEND id=%llu\\n", (unsigned long long)id);\n'
        '            auto it = conns_.find(id);',
    ),
    # Why the reply was refused, with both flags
    (
        '            if (c->isClosed.load(std::memory_order_acquire) || c->closing) {',
        '            if (c->isClosed.load(std::memory_order_acquire) || c->closing) {\n'
        '                std::fprintf(stderr, "[t] REFUSE id=%llu closing=%d closed=%d\\n",\n'
        '                            (unsigned long long)c->id, (int)c->closing,\n'
        '                            (int)c->isClosed.load(std::memory_order_acquire));',
    ),
    # Every park, whatever decided it
    (
        'inline void SubReactor::parkClose(Conn* c, const char* reason) noexcept {',
        'inline void SubReactor::parkClose(Conn* c, const char* reason) noexcept {\n'
        '    std::fprintf(stderr, "[t] PARK id=%llu reason=%s\\n",\n'
        '                (unsigned long long)c->id, reason ? reason : "?");',
    ),
    # The application asking for the close
    (
        'inline bool SubReactor::submitClose(uint64_t id) noexcept {',
        'inline bool SubReactor::submitClose(uint64_t id) noexcept {\n'
        '    std::fprintf(stderr, "[t] SUBMIT_CLOSE id=%llu\\n", (unsigned long long)id);',
    ),
]


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    src, dst = sys.argv[1], sys.argv[2]
    text = open(src, encoding="utf-8").read()
    missed = 0
    for old, new in PATCHES:
        if old not in text:
            print("MISS (header drifted?):", old.splitlines()[0][:70])
            missed += 1
            continue
        text = text.replace(old, new, 1)
    open(dst, "w", encoding="utf-8").write(text)
    print("patched %s -> %s (%d patch(es) missed)" % (src, dst, missed))


if __name__ == "__main__":
    main()
