// Minimal <sys/wait.h> for verify/stub/. Only what a probe that forks a child
// and reads its fate needs; the header is included for the same reason every
// other stub here exists: Windows has no waitpid and no WIFSIGNALED, and a
// probe that cannot be compiled is a probe that cannot be checked.
//
// Status word layout matches Linux: low 7 bits are the signal (0 means the child
// exited), bits 8-15 are the exit code.
#pragma once

#define WNOHANG   1
#define WUNTRACED 2

extern "C" int waitpid(int pid, int* status, int options);

#define WIFEXITED(s)   (((s) & 0x7f) == 0)
#define WEXITSTATUS(s) (((s) >> 8) & 0xff)
#define WIFSIGNALED(s) ((((s) & 0x7f) + 1) >> 1) > 0
#define WTERMSIG(s)    ((s) & 0x7f)
#define WIFSTOPPED(s)  (((s) & 0xff) == 0x7f)
#define WSTOPSIG(s)    (((s) >> 8) & 0xff)
