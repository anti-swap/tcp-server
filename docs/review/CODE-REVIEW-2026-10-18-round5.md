# 第五轮审查：tcp-server-mt.h（审查基线 `7454530`）

审查时间：2026-10-18
范围：`tcp-server-mt.h`（2358 行）+ `verify/` 套件覆盖面
方法：静态通读 + 对每条怀疑单独写探针实测（与前四轮同一条规矩：没有运行时证据的不写成缺陷）

前四轮已关闭的主题本轮不再重复：关闭语义与 FIN 顺序、doTick 迭代期关闭、drain 的双语义零值、
停靠 MOD 的成功/失败两路、读预算下的 backlog、`framesSent` 计量点、原子序、shim 与主机侧编译检查。

**状态：7 条全部处置完毕**（5 次提交 + 报告）。下面的表格已更新为处置结果，
各条的"处置"小节写在原分析之后，原分析一字未改——结论变了的地方在处置里说明，
而不是把当初的推理擦掉。

## 摘要

| 级别 | 条目 | 性质 | 证据 | 处置 |
|---|---|---|---|---|
| P2 | R5-1 idle 时钟只在**完整帧**解析时刷新 | 行为缺陷，实测 | `verify/idle_semantics.cpp` | 已修 `712693a`；边界用例 `fa3e111` |
| P2 | R5-2 `teardownClosed()` 在 noexcept 里做 4 次可能抛出的 `push_back` | 稳健性 | 注入 `probe_teardown_alloc.cpp` | 已修 `4ef774c` |
| P1 | R5-3 测试盲区：PING/PONG 零覆盖；idle 语义零覆盖 | 测试盲区（R5-1 的藏身处） | `verify/ping_pong.cpp`、`verify/idle_semantics.cpp` | 已固化 `2c53ddd`、`7100fa7` |
| P3 | R5-4 `submitSend()` 的 wbuf 容量拒绝分支不可达，且与相邻分支计量约定不一致 | 死代码 + 不一致 | `probe_wbuf_capacity_branch.cpp` | 已处置 `b7463cf` |
| P3 | R5-5 `handleRead()` 两个 recv 后分支里的 `parseFrames` 是死代码 | 死代码 | 静态 | 已删 `b7463cf` |
| P3 | R5-6 `requestStop()` 不唤醒 reactor，停止延迟最长一个 tick | 时序 | 静态 | **不改**，理由写入代码 `b7463cf` |
| P3 | R5-7 PING 分支因 pendingBytes 上限拒绝时，reason 写作 "wbuf full on pong" | 可观测性 | 静态 | 已修 `b7463cf` |

---

## R5-1（P2）"空闲"只看完整帧：正在发送的对端会被判死

`Conn::lastRecvMs` 全文件只有一处写入：`parseFrames()` 里消费掉一个**完整帧**的那一行
（`c->lastRecvMs = nowMs()`，`tcp-server-mt.h:1324`）。`handleRead()` 里 `recv` 成功
之后并不刷新它，而 `doTick()` 的空闲判定就是 `c->lastRecvMs < cutoff`（`:1744`）。

于是"对端正在发字节但还没凑成一帧"和"对端沉默"在空闲时钟上是同一件事。
`bytesRecv` 一路在涨，`idleTimeouts` 也照样把它关掉。

实测（`verify/evidence/probe_idle_halfframe.cpp`，`idleTimeoutMs=1000`、`tickMs=50`，
同样的字节率、同样的 2000B/150ms 节奏，只有"是否成帧"不同）：

```
half-frame   sent  14000 B in  1055 ms | peer FIN'd: yes | bytesRecv 12008 idleTimeouts 1 framesRecv 0
               hung up at 1055 ms
full-frames  sent  60000 B in  4519 ms | peer FIN'd: no  | bytesRecv 58232 idleTimeouts 0 framesRecv 29
```

半包那一侧：发了 14000 字节、一次都没停过，第 1055 ms 被服务端挂断，`bytesRecv` 明明是 12008。
完整帧那一侧：同样的速率跑满 4.5 秒，无事发生。

现实意义不是"半包攻击"，而是**一个合法的大帧在慢链路上分多次抵达**：默认 `idleTimeoutMs`
是 120 秒，一个 64KB 的帧若传得比这更慢，就在传输途中被判空闲——而它下一秒就把剩下的字节发过来了。

**建议修法**：`handleRead()` 的 `n > 0` 分支里，在 `rbuf.write` 之后加 `c->lastRecvMs = nowMs();`。
一行，语义从"没有成帧的活动"改回"没有任何字节到达"。注意这会连带改变一件事：只发不成帧字节
的对端不再会被空闲回收（它本来就应当被 `rbuf` 满、或者 TCP keepalive 处理），这一条要写进注释。

### 处置（已修，`712693a` + `fa3e111`）

按上面做了：`handleRead()` 的 `n > 0` 分支里 `c->lastRecvMs = nowMs();`，`Conn` 上的注释改写为
"最后一次收到字节"，并写明它带来的豁免。`parseFrames()` 里那一行保留（它是唯一的逐帧时间戳，
现在只是对超时而言冗余，不再是唯一来源）。

红→绿用同一份 `verify/idle_semantics.cpp`（已纳入 `run-in-wsl.sh`）：

| 用例 | 修复前 | 修复后 |
|---|---|---|
| A 半包（2000B/150ms，永不成帧） | 发出 10000 B、收到 8008 B，`idleTimeouts 1`，**被挂断** | 全 PASS |
| B 沉默 | PASS | PASS（沉默仍被回收） |
| C 完整帧 | PASS | PASS |

**"连带改变"的实测答案（待核实 #1，`fa3e111` 的 case D）**：豁免是有界的，且不需要新增配置。
一个帧不能被无限拖住——帧头长度受 `MAX_FRAME_PAYLOAD` 限制（超出的在帧头到达时就以
"frame too large" 结束），而 `rbuf` 被 `start()` 要求至少能装下最大的合法帧，因此
`closeConn(c, "rbuf full with incomplete frame")` **在 `start()` 的校验下不可达**（这是本轮
顺带查出的一条：`rbufCapacity < FRAME_HEADER_SIZE + MAX_FRAME_PAYLOAD` 会被拒绝）。
于是"卡在帧里"的对端只要在发就活着，一旦停止就是普通沉默对端：
发到 64000/65536 字节后停下 → `idleTimeouts` 从 0 变 1，正常挂断。

（第一版 case D 声明了超过 `MAX_FRAME_PAYLOAD` 的长度，拿到的是"frame too large"——正是
这个错误结果暴露了上面那条不可达性。）

---

## R5-2（P2）`teardownClosed()` 在 `noexcept` 里做 4 次可能抛出的 `push_back`

`:1650-1653`：

```cpp
pendingCloses_.push_back(keepAlive);
pendingCloseIds_.push_back(c->id);
pendingClosePeers_.push_back(c->peer);          // std::string 拷贝：会分配
pendingCloseReasons_.push_back(reason ? reason : "");  // 同上
```

四个函数全是 `noexcept`（`teardownClosed` ← `closeConn` ← `handleRead`/`handleWrite`/`parseFrames`），
`flushCloseCallbacks()` 的注释自己写着"All four vectors are cleared together so their indices
always agree"——而第 2 次 `push_back` 抛 `bad_alloc` 时，第 1 次已经进去了：索引错位，
并且异常穿过 `noexcept` 直接 `std::terminate()`。

同一个文件里 `submitNewConnection()` 对 `conns_`/`fdIndex_` 的插入是包在 `try` 里的
（`:848-858`），理由是"回滚必须走同一条路"。这里是同一类分配，没有同类的保护。
概率很低（只有 OOM），但后果是进程终止而不是丢一条连接，且它正好落在**关闭**路径上——
负载高、内存紧的时候最先被走到的那条路。

**建议修法**：把四个 `push_back` 包进 `try`；更简单也更彻底的是把"活跃引用 / id / peer / reason"
合成一个 struct，一次 `emplace_back` 保证原子性。

### 处置（已修，`4ef774c`）

两条都做了：四个平行向量合成 `std::vector<ClosedConn>`（部分插入不再可表示），插入本身包在
`try` 里，失败时记新增的 `closeCallbacksDropped` 而不是让异常穿过 `noexcept`。

红→绿用链接期注入（`verify/evidence/probe_teardown_alloc.cpp`）：`--wrap=close` 在
`unregisterConn()` 关闭描述符后布防（它与通知入队之间没有任何分配），`--wrap=_Znwm` 让下一次
`operator new` 抛 `bad_alloc`，场景在 fork 出的子进程里跑，这样"进程是否还活着"可以被观测：

| | 修复前 | 修复后 |
|---|---|---|
| 结果 | `terminate called after throwing 'std::bad_alloc'` → **SIGABRT** | 存活，`watched closes 1`，`closeCallbacksDropped 1` |
| 后续连接 | — | 关闭回调照常运行（这一条区分"吸收了"和"回调坏了"） |

待核实 #2（真实 OOM 下的表现）由此关闭：注入的就是 `bad_alloc`，落点就是那一行。

---

## R5-3（P1，测试盲区）PING/PONG 与 idle 语义：零覆盖

`grep -rn "PING\|PONG\|pong" verify/` → 无结果。整个 `parseFrames()` 的 PING 分支
（reserve 的 CAS 循环、容量拒绝时 `closeConn("wbuf full on pong")`、closing 时丢弃并记
`framesDroppedOnClose`）从来没有被任何测试执行过一次。它是读路径里条件最多的一段。

本轮补跑了一次（`verify/evidence/probe_ping_path.cpp`），结果**行为正确**：

```
healthy      PING sent   5 | PONG received   5 | framesRecv 5 framesSent 5 bytesSent 40   (= 8*5)
whileClosing PING sent 5   | framesDroppedOnClose 5 framesSent 0 aborted 0 closed 0
```

计量自洽（PONG 计入 `framesSent`，`bytesSent == 8 × PONG`），关闭中的 PING 被丢弃而非越过 FIN 应答。
所以这条不是"代码有错"，是**"正确但没有守护"**：它随时可以被改坏而整套回归仍然全绿。

idle 那一侧同理：`verify/` 里唯一用到 `idleTimeoutMs` 的地方是 `close_semantics.cpp:586`，
用途是"让关闭停靠一会儿"，没有任何用例断言过空闲语义本身——R5-1 就藏在这里。

**建议**：把上面两个探针固化成 `verify/ping_pong.cpp` 和 idle 用例（后者先转红再修），纳入
`run-in-wsl.sh`。

### 处置（已固化，`2c53ddd` + `7100fa7`）

两个都进了套件并纳入 `run-in-wsl.sh`：

- `verify/ping_pong.cpp`：健康连接上 5 PING → 5 PONG（形状正确）、`framesSent 5`、
  `bytesSent 40 == 8 × 5`；关闭中的 PING 走 `framesDroppedOnClose`，不计入 `framesSent`。
- `verify/idle_semantics.cpp`：R5-1 的红→绿载体（见上）。

固化时抓到一条我自己写错的断言（`7100fa7`）：`caseWhileClosing` 从 `closeConnection()` 返回的
那一刻起断言 `framesSent == 0`，而 `closeConnection()` 只是**请求**，关闭要经 reactor 队列才落到
连接上，已经在路上的 PING 被应答是合法的。三次 asan 跑出 5/0、3/2、4/1——用例在断言这个竞态
本身。改成"PING 到出现一次 dropped（关闭生效的可观测点）之后再取基线"：五连跑 `dropped +5`、
`framesSent` 不变。

---

## R5-4（P3）`submitSend()` 的 wbuf 容量拒绝分支不可达，且与相邻分支不同账

`:912-916`：

```cpp
if (c->wbuf.writable() < f.size()) {
    c->pendingBytes.fetch_sub(f.size(), ...);
    server_->statsRef().queueDrops.fetch_add(1, ...);   // 没有 bytesDropped
    return;
}
```

紧挨在上面的"已关闭/关闭中"分支两个计数器都加，注释还专门写了"一个拒绝，两个问题，两个计数器"。
这个分支只加 `queueDrops`。

**它触发不了。** 理由：`start()` 把 `maxPendingBytes` 夹到 `wbufCapacity` 以内；一帧的预留
直到写进 wbuf 为止都留在 `pendingBytes` 里；wbuf 的内容是 `pendingBytes` 的子集。所以任务执行时
恒有 `pendingBytes >= wbuf.readable() + frame.size()`，即 `wbuf.writable() >= frame.size()`。

实测（`verify/evidence/probe_wbuf_capacity_branch.cpp`，对 `tcp-server-mt.h` 该分支临时插桩 `fprintf`）：
8 个并发生产者、5229 次入队成功、35369 次拒绝、38 MB 真实发送、对端慢读、中途触发关闭——
**该分支命中 0 次**，`queueDrops == refused`（35369 == 35369）全部来自 `sendTo` 的额度检查，
`bytesDropped` 为 0。

所以：不一致是真的，运行时影响是零。**建议**二选一：删掉这个分支（换成 `assert`，把上面的
不变式写成代码），或者补齐 `bytesDropped` 并说明它在什么配置下才可能触发。现在的状态是
"一段永远不会跑的分支，带着一份和邻居不同的记账约定"。

### 处置（已处置，`b7463cf`）

选了"补齐 + 写下来"，没选删掉：这个分支的替代行为是往 wbuf 里写半帧，那会破坏帧流，
不值得为"它跑不到"换掉。现在它同时记 `bytesDropped` 和 `queueDrops`——与相邻分支同一约定
——并把不可达的推导写在分支上方。

不可达性在改动后又复测了一次（同一套插桩 + 压力）：8 并发生产者、5349 帧入队、36205 次拒绝、
39 MB 真实送达、对端慢读、中途触发关闭 → **命中 0 次**。

---

## R5-5（P3）`handleRead()` 里两个 recv 之后的 `parseFrames` 是死代码

`:1137`（EAGAIN 分支）和 `:1121`（`n == 0`，对端 FIN 分支）都在 `recv` 返回之后又调一次
`parseFrames`。但能走到 `recv` 的前提是：循环顶部的 `parseFrames` 已经把 rbuf 里能解析的都解析了
（否则 `framesLeft <= 0` 的检查会在下一行就 `scheduleRead` 并 return）。这两次 `recv` 都不往 rbuf
里加字节，所以 rbuf 内容与上一次解析时完全相同——没有新字节，`parseFrames` 不可能解析出新帧。

无害，但它读起来像"这里还需要再补一次解析"，而下一个人会因此以为存在某种漏解析的情况。
**建议**删掉，或改成一句注释说明它为什么是冗余的（和 `:1131` 那个 `assert(framesLeft > 0)` 一样，
把不变式写出来）。

### 处置（已删，`b7463cf`）

删掉，两处都换成写下来的不变式（"能走到 recv 的都刚被解析过，而 recv 没加字节"）。
`close_semantics` 全绿——case E 就是"对端 FIN 之后还欠着应答"的那条，若有帧被漏在 FIN 后面
没解析，它会先失败。

---

## R5-6（P3）`requestStop()` 不唤醒 reactor

`requestStopInternal()`（`:628-634`）只做两件事：`Running → Stopping`、`wakeAcceptLoop()`。
reactor 的 `loop()` 退出条件是 `server_->isRunning()` 为假，而它们此刻正阻塞在
`epoll_wait(..., tickMs)` 上——没人写它们的 eventfd，最长要等一个 `tickMs`（默认 500ms）才醒来。

`stop()` 不受影响（它对每个 reactor 调 `wakeup()` 再 join），只有单独用 `requestStop()` + `wait()`
这条路径才有这段延迟。

### 处置（不改，理由写进代码，`b7463cf`）

待核实 #3 先做了：`grep requestStop|wait()` 在 `verify/` 下零结果，没有用例依赖它。

然后决定不改，因为修法比缺陷危险：唤醒 reactor 要从 `requestStopInternal()` 触碰 `reactors_`，
而这个函数跑在三种线程上——调用者、accept 循环、以及回调（`SubReactor::stop()` 把回调内的
停止路由到这里）。`reactors_` 只由 `stop()` 在 `lifecycleMutex_` 下改动，而回调路径**已经持有**
那把锁，所以这里只剩两个选择：加锁会自锁死锁，不加锁则与 `clear()` 竞争。

收益也够小：只有 `requestStop()` + `wait()` 这一对看得到这段延迟，而 `wait()` 本身是 200ms
轮询，比它想省下的一个 `tickMs` 还粗。结论写成了 `requestStopInternal()` 上的注释，下一个人
不必再推一遍。

---

## R5-7（P3）reason 与实际原因不符

`:1341-1346`：PING 的拒绝条件是 `!reserved || c->wbuf.writable() < pong.size()`，
两种情况都记 `closeConn(c, "wbuf full on pong", false)`。而按 R5-4 的论证，`writable < pong.size()`
那半边不可达，**实际会发生的只有 `!reserved`，也就是 `pendingBytes` 撞上了 `maxPendingBytes`**
——和 wbuf 一点关系没有。关闭回调里送出去的理由会指向错误的方向。

### 处置（已修，`b7463cf`）

拆成两个分支、两个理由：`"pending bytes at limit on pong"`（会发生的那个）与
`"wbuf full on pong"`（按 R5-4 不可达的那个）。顺带把 `pendingBytes` 的回退挪到了真正
预留成功的那半边——原来两个原因共用一次 `if (reserved)` 减法。

---

## 待核实清单（三项全部关闭）

- `[已核实 2026-10-18] R5-1 修复后，"只发不成帧字节"的对端最长可存活多久` ——
  见 R5-1 处置里的 case D：不是靠 TCP keepalive，而是靠"帧头长度上限 + rbuf 恒能装下最大合法帧"
  这两条把"卡在帧里"变成有界状态；停止发送即回到普通沉默对端。**附带查出
  `closeConn(c, "rbuf full with incomplete frame")` 在 `start()` 的校验下不可达**（新条目，见下）。
- `[已核实 2026-10-18] R5-2 在真实 OOM 下的表现` —— `probe_teardown_alloc.cpp` 注入的正是
  `bad_alloc`，落点正是那一行：修复前 SIGABRT，修复后存活并计数。
- `[已核实 2026-10-18] R5-6 的延迟是否有用例依赖` —— `verify/` 下 `requestStop()`/`wait()`
  零引用。最终不改，理由写进代码。

## 本轮新增的开放项

- `closeConn(c, "rbuf full with incomplete frame", false)`（`handleRead`）在当前校验下不可达：
  `start()` 拒绝 `rbufCapacity < FRAME_HEADER_SIZE + MAX_FRAME_PAYLOAD`，而任何能通过
  `len > MAX_FRAME_PAYLOAD` 检查的帧都能整体装进 rbuf，所以"rbuf 满且帧未完成"不可能成立。
  与 R5-4 同一类（不可达但无害），未在本轮改动，记在此处。

## 建议顺序（已按此执行）

1. ~~R5-1~~ → `712693a`（修复）+ `fa3e111`（边界用例）
2. ~~R5-3~~ → `2c53ddd`（PING 固化）+ `7100fa7`（修正竞态断言）
3. ~~R5-2~~ → `4ef774c`
4. ~~R5-4 / R5-5 / R5-7~~ → `b7463cf`（R5-6 的决定也在这条里）
5. ~~R5-6~~ → 不改，理由写入代码

## 本轮产物

套件成员（纳入 `run-in-wsl.sh`，每次回归都跑）：

- `verify/idle_semantics.cpp` —— A 半包 / B 沉默 / C 完整帧 / D 半包后停滞
- `verify/ping_pong.cpp` —— 健康路径的 PONG 与计量，关闭中的丢弃
- `verify/evidence/run_one.sh` —— 单跑一个用例并报告真实退出码（内联 `bash -lc '...$?'`
  会被外层展开，这次就报出过 `exit=0` 而实际是 1）

证据探针（按需使用，不进回归）：

- `verify/evidence/probe_teardown_alloc.cpp` —— R5-2，`--wrap=close` + `--wrap=_Znwm`，fork 子进程观测
- `verify/evidence/probe_wbuf_capacity_branch.cpp` —— R5-4，配合对 `submitSend` 的临时插桩使用

已删除：`probe_idle_halfframe.cpp`、`probe_ping_path.cpp`（测量已被会断言的套件成员接管）。

## 回归

`ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run-in-wsl.sh`：plain / asan+ubsan / tsan ×
{ close_semantics(默认, CS_REACTORS=1), idle_semantics, ping_pong, backpressure, fault_injection }
= 18 种配置全 `rc=0`；cpplint 对照 2 findings、头文件 0；退出码哨兵 9 项 OK。
Windows 侧 `bash verify/build.sh`：14 个 TU（含本轮新探针）、无警告、exit 0
（`verify/stub` 补了 `fork`/`_exit`/`<sys/wait.h>`）。
