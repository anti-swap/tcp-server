# 第四轮：外部审查核实（`finishPeerClosed` 的停靠 MOD 失败）

本轮不是一次完整复审，只有一条：用户带来一张外部审查截图，断言
`finishPeerClosed` 的“停靠”分支在 `epoll_ctl(MOD)` 失败后只加计数、不拆除，
会导致 busy loop，并称“前两轮都说过，这版还没改”。

结论：**断言成立，已修复（`0666ba8`）**。截图对因果链的描述与代码逐行吻合；
但对“前两轮都说过”这一点需要更正，且它漏掉了一种后果相反的失败模式。

---

## 1. 代码事实

`tcp-server-mt.h` `finishPeerClosed()`，对端 FIN 且 `wbuf` 非空时：

```cpp
if (epoll_ctl(epollFd_, EPOLL_CTL_MOD, c->fd, &wev) == 0)   // wev = EPOLLOUT only
    c->writeArmed = true;
else
    stats.epollCtlErrors++;
// A failed MOD keeps EPOLLIN armed: the drain spins, nothing breaks.
```

LT 模式下对端 FIN 让 socket 永久可读，`EPOLLIN` 未摘 → 每次 `epoll_wait` 立刻返回
→ `handleRead` → `recv`=0 → `finishPeerClosed` → 同一个 MOD 再失败。一次
`epoll_wait` 一轮，直到某个截止条件到来。

## 2. 复现手段

`verify/evidence/probe_fin_mod_failure.cpp`，`-Wl,--wrap=epoll_ctl`，只拦一种调用形状：
`MOD && (events & EPOLLOUT) && !(events & EPOLLIN)`（`armWrite` 是 IN\|OUT，
`disarmWrite` 是 IN，故不会误伤健康调用）。负载：16MB 应答 + 对端 `SO_RCVBUF=2048`
且不读，250ms 后 `shutdown(SHUT_WR)` 并保持 socket 打开（这样不会走 RST 路径）。

## 3. 红 → 绿

| 注入 | 指标 | 修复前 | 修复后 |
|---|---|---|---|
| 不失败（对照） | `eofReads` / CPU | +1 / 0% | +1 / 0% |
| MOD 返回 ENOMEM | `eofReads` / `epollCtlErrors` / CPU | **+67835 / +67835 / 97%** | +1 / +1 / 0% |
| MOD 返回 ENOMEM | 对端补齐读取后拿到的字节 | 16781312 / 16781312，但白烧一核 | 4096，连接已拆除（`aborted=1`） |
| MOD 返回 ENOENT | 对端补齐读取后拿到的字节 | **3145728 / 16781312，随后挂死** | **16781312 / 16781312** |
| MOD 返回 ENOMEM（完整周期，idle 1.5s + linger 0.5s） | 失败调用次数 / 结局 | 314903 → idle 超时 → 仍停靠 → linger 强拆，`bytesDropped=13635584` | 1 → 就地拆除，`idleTimeouts=0` |

修复前后各跑一次，量级稳定（三次数值 59775 / 67078 / 67835）。

## 4. 修复（`0666ba8`）

与 `armWrite` 完全同构——同一个系统调用，此前文件里已有两个地方给出裁决，
只有这里选了宽松策略：

- `ENOENT`：用 `ADD` 收养 `EPOLLOUT`（`writeArmed = true`）。这不是可选的补救，
  而是必需：`ENOENT` 意味着 fd 不在任何兴趣表里，不收养就是永久静默。
- 其余错误：`epollCtlErrors++` + `closeConn(c, "epoll park registration failed", false)`。

没有第三种选择：半注册成功的停靠唯一能做的事就是自旋。

**取舍，写在这里而不是留在注释里：** 非零错误下的拆除会丢掉仍未发出的字节
（本次 13.6MB）。备选方案是 `DEL` 掉 fd 换掉自旋、再由一个停靠重试集合在
`doTick` 里反复重试注册——它需要新增状态，且与 `armWrite`/`disarmWrite` 对同一
系统调用的裁决相反。在 ENOMEM 级别的触发概率下，选择一致性。若将来
`epollCtlErrors` 在生产里真的动过，这个取舍就是第一个该复查的地方。

## 5. 对截图的三处更正/补充

1. **“前两轮都说过”不准确。** 前两轮只把 MOD **成功**路径列为条目并修复
   （P1-1b / `a297d9b`）；失败分支从未成条目，`nothing breaks` 那句注释正是那次
   修复时写下的。所以“这版还没改”成立，“说过”只在 P1-1b 这个主题层面成立。
2. **“busy loop 直到 linger/idle 超时”低估了。** 完整周期是两段计时叠加：
   idle 超时触发优雅关闭后连接**仍在停靠**（MOD 继续失败），要再等 `closeLingerMs`
   才被强拆；自旋贯穿两段。
3. **截图只说自旋，漏了 ENOENT 的静默。** 见上表第 4 行：修复前对端只能拿到
   3MB 中的已发送部分然后挂死，这是比自旋更严重的一种（数据不到，且谁也不会发现）。
   截图的建议“ENOENT 可以用 ADD 恢复，其他错误必须 abort”是对的，但没说清
   ENOENT 的后果方向相反——它不是“也自旋”，而是“全静默”。

---

# 第二轮外部审查：N1 / N2 / N3

同一轮里用户带来第二张截图，列了三条。核实结论：**N1 成立（已修 `cd52cf8`）、
N2 成立（已修 `1b173a6`）、N3 只是可选加固（已在 `1b173a6` 里补注释）**。

## N1 — `drainInputThenClose` 在 backlog 未读完时就停靠（P2）

**断言与实际的差别**：不是 FIN 抢在已排队应答之前，而是 backlog 尾部在 `closing`
置位**之后**才被派发，其应答被 `submitSend` 拒绝。根因是 `handleRead` 每轮只处理
`kFrameBudget=256` 帧 / `kReadBudget=64KB`，而 `drainInputThenClose` 只调它一次。

证据 `verify/evidence/probe_close_backlog.cpp`（echo，首个帧触发 `closeConnection`，
对端读到 EOF）：

| N | 修复前应答 | 修复前 queueDrops / bytesDropped | 修复后 |
|---|---|---|---|
| 100 / 256 / 300 | 全应答 | 0 / 0 | 全应答 |
| 600 | **512** | **88 / 3520** | 600 / 0 / 0 |
| 1200 | **512** | **195~688**（时序相关） | 1200 / 0 / 0 |

512 = 两轮预算。`dispatched` 恒等于 N：那些请求**进了 handler、跑了处理、应答被拒**，
正是 L1488-1493 声称该函数要消灭的失败，只是从“队列顺序”换成了“读预算”这条路径。

**修复**：input 仍未读完时不再停靠，而是再排一轮。轮次由 `Conn::closeInputBytes`
界定——在关闭请求那一刻按 rbuf 已缓冲字节数取一次快照，每轮按“已派发帧数 ×
FRAME_HEADER_SIZE”这个**下界**扣减（下界 = 只会让界偏松，不会提前收紧）。新到的
输入不延长它，所以持续发送的对端仍不能把关闭无限拖住。

**这条放宽必须有守卫**：原设计的“至多一轮”就是为了防止这件事。探针里的
`runHostile()`（请求关闭后对端继续每 1ms 发 256 帧）实测 **FIN 在 87ms 后到达**
（期间又服务了 47 个 burst，之后到达的按既有规则拒绝）——界的作用在此，它不是
红→绿用例，而是防止将来有人把界去掉的守卫。

## N2 — `disarmWrite` 两条注释都与代码不符（P3，纯文档）

- “Targeted on the EPOLLOUT bit, **not on an 'armed' flag**” —— 下一行就是
  `if (!c->writeArmed || c->fd < 0) return;`（注释由 `378183f7` 加，守卫来自更早的
  `64d2435`，从未对齐）。它想说的是“标志是目标态而非开关”，已改写。
- “Still has bytes to flush: keep the interest…” —— 它描述的
  `if (!c->wbuf.empty() || c->fd < 0) return;` 由 `478e074` 引入、**被 `378183f7`
  删除**，注释留在原地。守卫是有意去掉的（wbuf 还有字节时摘写兴趣等于把它们搁死），
  两个调用点都已保证 wbuf 为空。已改写为说明“为什么这里不该有这个守卫”。

行为未变，两个调用点（`handleWrite` 排空后、`finSent` 路径）本来就在 wbuf 已空之后。

## N3 — `finishPeerClosed` 的 `fd < 0` 兜底（P4）

不可达：`fd = -1` 只出现在 `unregisterConn()`（位于 `teardownClosed` 的 `isClosed`
CAS **之后**）和 `Conn` 析构兜底，而函数开头已用 acquire 读过 `isClosed`。
加注释说明为什么上面的 `if (c->fd >= 0)` 守卫要留着（将来若出现第二条 detach 路径，
它才不会变成对 fd -1 的盲调用）。代码未动。

## 6. 附带发现：主机侧编译检查自 `9c8200a` 起一直在失败（`0e445f7` 已修）

`verify/build.sh`（Windows 侧编译检查）在 HEAD `9c8200a` 上 exit 1：

```
error: 'RLIM_INFINITY' was not declared in this scope
error: 'setrlimit' was not declared in this scope
```

`start()` 会在监听前抬 `RLIMIT_NOFILE`（`9c8200a` 之前引入），而 `verify/stub`
只声明了 `getrlimit`——`setrlimit` 和 `RLIM_INFINITY` 都没有。它是这次唯一
一条**别人已经说对、我却一直没看见**的问题：Linux 侧每轮全绿，而这个脚本
不在 `run-in-wsl.sh` 里，也没人单独跑它。

补齐 shim 之后又冒出四处缺口，全都在脚本**从未编译过**的文件里：`accept`、
`SO_RCVBUF`、`MSG_DONTWAIT`、`usleep` / `struct timeval`。所以真正的缺陷不是
某个符号，而是覆盖面——它只编译 2 个 TU。现在它编译 `verify/` 下全部 `.cpp`
（含 evidence 探针），共 10 个 TU；下一个缺口会在它被需要的当天暴露，而不是
等哪天有人手跑一次。

两处值得一提的细节：

- `struct timeval` 取自宿主的 `<_timeval.h>`，不在 shim 里重写。mingw-w64 已经
  声明过（字段与 Linux 同形），任何经 `<chrono>` 触到 `<ctime>` 的 TU 也都会
  拿到它——再定义一份是硬错误（实测撞过）。
- `struct rlimit` 移到 `extern "C"` 块**之前**。只在块内被提名的话，那两个
  原型检查的是一个头文件永远看不到的类型。

**它没能接进 `run-in-wsl.sh`，这点如实记录**：WSL 里能执行那个 Windows 的
`g++.exe`，但 Windows 进程从 Linux 启动时拿不到 Windows 环境，`cc1plus` 起不来
（`CreateProcess: No such file or directory`）；补 `SYSTEMROOT` 与 Windows 形态
的 `PATH` 仍无效，经 `cmd.exe` 包装又被环境的安全策略拦下。所以它是 Windows 侧
单独一步，`run-in-wsl.sh` 现在明确写出"本轮对它一无所知"，而不是让人以为覆盖到了。

**对照实验（证明该检查会失败）**：删掉 `setrlimit` 声明 → `build.sh` exit 1；
恢复 → exit 0。另加 `verify/evidence/check_build_exit.sh`：独立运行 `build.sh`
并用它的退出码再退出，避免被管道吞掉。

## 7. 回归

三次（P1-1c 之后、N1+N2/N3 之后、build 修复之后）各跑一遍
`ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run-in-wsl.sh`：

- plain / asan+ubsan / tsan × { close_semantics(默认, CS_REACTORS=1), backpressure,
  fault_injection } = 12 种配置，`rc=0`
- cpplint：对照文件 2 findings（证明该检查可失败），`tcp-server-mt.h` 0 findings
- 退出码哨兵 9 项全 OK

主机侧另跑 `bash verify/build.sh`（Windows shell，不在 WSL 回归内）：10 个 TU、
无警告、exit 0。
