#!/usr/bin/env bash
# Exit-code plumbing probe.
#
# Reason this exists: a command handed to WSL as `bash -lc '<string>'` reported
# $? = 0 for `false`, for `(exit 3)` and for a program that returns 7. Not one
# of those is 0. Every pass/fail decision in run-in-wsl.sh reads an exit code,
# so until this agrees with itself those decisions mean nothing.
#
# Usage: bash verify/probe_exit_code.sh
# Prints OK/WRONG per check; leaves rc wrong if any environment here lies.

bad=0
check() {   # check <label> <expected-rc> <command...>
    local label="$1" expected="$2"; shift 2
    "$@" >/dev/null 2>&1
    local got=$?
    if [ "$got" = "$expected" ]; then
        printf "  [OK]    %-28s rc=%s\n" "$label" "$got"
    else
        printf "  [WRONG] %-28s rc=%s expected %s\n" "$label" "$got" "$expected"
        bad=$((bad + 1))
    fi
}

echo "1. shell builtins and obviously-failing binaries"
check "false"            1 false
check "/bin/false"       1 /bin/false
check "subshell exit 3"  3 bash -c 'exit 3'
check "true"             0 true
check "ls missing"       2 ls /nonexistent-xyz

echo "2. a compiled program, because that is what the suite actually runs"
probe=$(mktemp -d)
cat > "$probe/rc.cpp" <<'EOF'
int main() { return 7; }
EOF
if g++ -std=c++17 "$probe/rc.cpp" -o "$probe/rc" 2>"$probe/build.err"; then
    check "compiled return 7" 7 "$probe/rc"
else
    echo "  [WRONG] the probe program did not build:"
    cat "$probe/build.err"
    bad=$((bad + 1))
fi

echo "3. the wrap the failure would arrive in: timeout, then the pipe the suite uses"
check "timeout: compiled 7" 7 timeout 60 "$probe/rc"
check "timeout: infinite sleep" 124 timeout 1 sleep 30
printf 'int main(){return 0;}\n' > "$probe/ok.cpp"
g++ -std=c++17 "$probe/ok.cpp" -o "$probe/ok" 2>/dev/null
check "timeout: compiled 0" 0 timeout 60 "$probe/ok"
rm -rf "$probe"

echo
if [ "$bad" -eq 0 ]; then
    echo "exit codes travel correctly through this invocation path"
else
    echo "$bad check(s) disagree with reality -- do not trust a pass/fail decision here"
fi
exit "$bad"
