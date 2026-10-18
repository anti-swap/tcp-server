#!/usr/bin/env python3
"""Break the `finSent => closing` invariant in a COPY of tcp-server-mt.h.

doTick() walks conns_ with a range-for while calling closeConn(). A graceful
close is only *parked*; one that has already sent its FIN goes straight to
teardownClosed(), which erases from conns_ -- invalidating the iterator the
range-for is holding. The reason that cannot happen today is an invariant
nothing enforces: every connection still in conns_ that has FIN'd is also
marked closing. It lives in parkClose(), handleWrite() and closeConn()
together, five functions away from the loop whose safety depends on it.

This injection makes doTick see exactly the state the invariant forbids --
what any refactor that touches one of those functions and not the others
would produce by accident. It is the difference between "we believe this is
safe" and "this cannot be unsafe".

Usage: python3 inject_fin_without_closing.py <header-in> <header-out>
"""
import sys

OLD = """    for (auto& kv : conns_) {
        Conn* c = kv.second.get();
        if (c->closing) {"""

NEW = """    for (auto& kv : conns_) {
        Conn* c = kv.second.get();
        if (c->finSent) c->closing = false;   // INJECTED: finSent => closing broken
        if (c->closing) {"""


def main() -> int:
    if len(sys.argv) != 3:
        sys.stderr.write(__doc__ or "")
        return 2
    src, dst = sys.argv[1], sys.argv[2]
    with open(src, "r", encoding="utf-8", newline="") as f:
        text = f.read()
    n = text.count(OLD)
    if n != 1:
        sys.stderr.write("expected exactly one doTick injection point, found %d\n" % n)
        return 3
    with open(dst, "w", encoding="utf-8", newline="") as f:
        f.write(text.replace(OLD, NEW))
    sys.stderr.write("injected: doTick now sees connections that FIN'd without closing\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
