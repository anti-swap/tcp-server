# 第二轮代码审查报告 — tcp-server-mt.h

- 审查对象：`C:\Users\root\WorkBuddy\Claw\tcp-server\tcp-server-mt.h`（2032 行，当前 HEAD `28a1615`）
- 审查时间：2026-10-18
- 审查模式：full（andrej-karpathy-skills 流程）
- 性质：**复审**。第一轮报告（CODE-REVIEW.md）所列 P0-1~P2-6 已全部修复并经 plain / ASan+UBSan / TSan / cpplint 验证通过；本轮在修复后的代码上重新扫描，只报告**仍然存在或新发现**的问题。
- 状态（2026-10-18 更新）：**全部已修复并按缺陷单独提交**，每个缺陷均走了红→绿。下方各条附了提交号与实测前后数据。

## 摘要

- 环境：git 仓库，无 `.coding-standards.md`（启用内置默认规范）；验证套件 `verify/` 完整（close_semantics / backpressure / fault_injection + 三 sanitizer + cpplint）。
- 闭环状态：✅ 正常（最近修改均有测试与 sanitizer 执行记录）。
- 缺陷统计：P0 = 0，P1 = 1，P2 = 4，待核实 = 2（另有 2 项在实现修复过程中被发现，见 P1-2 / P1-3）。
- 总体结论：修复后的实现质量高——关闭协议、内存序、计数口径、故障注入闭环都有明确的论证注释且与代码一致。剩余问题集中在**半关闭状态的 epoll 事件管理**与若干边界配置校验。

## 缺陷明细（按 P0 → P1 → P2）

### P0：无

### P1-1：对端半关闭且 wbuf 未排空时，reactor 忙轮询 — **已修复 `a297d9b`**

- 位置：`finishPeerClosed()`（L1082-1094）+ `loop()` 事件分发（L1518-1521）
- 机理：对端发 FIN 后 `recv` 恒返回 0。`finishPeerClosed` 在 `wbuf` 非空时直接 `return`，连接保持注册且 **EPOLLIN 一直挂着**。level-triggered 下 epoll_wait 每次都立即返回这个 EPOLLIN，worker 在"recv→0→return"上空转，直到 wbuf 排干（慢对端 + 256KB wbuf 时可达秒级），期间该 reactor 一个核被烧掉。
- 影响面：每个"对端先半关闭、我们还欠它字节"的连接都会触发，属于常见路径而非边角。
- 建议（已采纳）：停靠分支把该 fd MOD 为仅 EPOLLOUT；补 `close_semantics` 用例 E。
- 证据：修复前 `eofReads +35823` / `+36903`（250ms 停靠窗口，约 14 万次/秒）；修复后 `+0`。

### P1-2（实现修复时发现，未被静态扫描列出）：对端半关闭时其应答被丢弃

- 位置：`finishPeerClosed()`（修复前 L1082）＋ `submitSend()`（始终入队，没有 reactor 线程直写快路径）
- 机理：handleRead 在 `recv==0` 后立刻核对已经在 `wbuf` 里的字节，但用户 handler 的应答是 `submitSend` **入队的任务**，此时还没落到 `wbuf`。于是连接被当场拆掉，FIN 发出，排在队列里的应答在下一轮发现连接已消失而被丢弃。原注释声称「handler queued 的应答已经在 wbuf 里」——这个前提是错的。
- 影响：request → `shutdown(SHUT_WR)` → 等应答（HTTP/1.0 式）这种合法模式，客户端收到的是「干净结束却没有答案」。
- 证据（红）：case E `wire ended with: FIN (orderly), 0 of 229432 bytes`。
- 修复 `7a6f523`：`wbuf` 空但 `pendingBytes > 0` 时延迟一轮再判定（`waitingOnReplies`，最多一次），FIFO 保证应答任务先跑进 wbuf，之后由停靠排空接手。修复后 `8194000 of 8194000`。

### P1-3（同源）：排空完成后没有确定性收尾

- 位置：`handleWrite()`：wbuf 排空且连接不是 `closing` 态时什么都不做
- 后果：靴带 peer 半关闭的停靠连接只在下一次偶发 EPOLLIN（或 idle 超时，默认 120s）才被拆掉。
- 修复 `a297d9b`：新增 `Conn::peerClosed` 标记，`handleWrite` 在排空完成处直接调用 `finishPeerClosed()`，不再等下一次事件。

### P1-4（由 case B 的一次偶发失败反向发现）：关闭请求抢跑在它自己的应答之前

- 位置：`drainInputThenClose()`（原 L1312，`served == 0` 分支）
- 机理：`served == 0`（已无新请求可读）不等于"没有欠账"。handler 产生的应答任务是**排队的**，
  `submitSend` 从不在 reactor 线程直写 wbuf，所以两个应答可能被这个 close 任务插在中间：
  应答 A 先跑并被放行，关闭任务随即 `parkClose`（`closing` 置位），应答 B 再跑时被拒绝。
  客户端看到的是"请求被受理、handler 跑过、只回来一半答案、然后一个礼貌的 FIN"。
- 证据（插桩还原的真实顺序，TSan）：

```
[t] SUBMIT_SEND id=22    <- handler 排入应答 A
[t] SUBMIT_CLOSE id=22   <- 应用的关闭落在 A 与 B 之间
[t] SUBMIT_SEND id=22    <- handler 排入应答 B
[t] TASK_SEND id=22      <- A 执行，放行
[t] PARK_IMMEDIATE id=22 <- 关闭任务：无新可读 -> 立刻 park
[t] TASK_SEND id=22      <- B 撞上 closing
[t] REFUSE id=22 closing=1
```

- 修复 `f176c1d`：`served == 0` 但 `pendingBytes > 0`（仍有已占额度未入 wbuf 的应答）时延迟一个
  队列轮次，`waitingOnReplies` 保证最多一次——与 P1-2 的处理完全对称（`pendingBytes` 的存在正是
  "还有应答在路上"的判据）。
- 验证（`close_semantics` case F，20 轮，TSan，`CS_REACTORS=1`）：修复前 8 次运行中有 4 次复现，
  修复后 8 次全部干净。工具：`verify/evidence/loop_close_race.sh`、`trace_close_race.py`。

### P2-1：`RLIMIT_NOFILE` 检查在 unlimited 下误报警 — **已修复 `e81c729`**

- 位置：`start()` L1621 / L1634
- `static_cast<int>(rl.rlim_cur)`：`rlim_cur` 为 `RLIM_INFINITY`（2^64-1）时强转 int 得到负数，两个 `< maxConnections + 64` 判断都为真 → 在"根本不设限"的机器上反而打印 "RLIMIT_NOFILE=18446744073709551615 too low" 的假警告。
- 已改为 `uintmax_t` 比较，`RLIM_INFINITY` 映射为 `UINTMAX_MAX`；普通提限路径（`1024 -> 65536`）行为不变。
- 说明：运行时复现需要 `CAP_SYS_RESOURCE`，本机不可得；本次依据整型转换规则判定，属静态确定缺陷。

### P2-2：`closeLingerMs <= 0` 时停靠关闭的连接永不清扫 — **已修复 `2006dd8`**

- 位置：`doTick()` L1444（`linger > 0` 才进过期判断）；`start()` 配置校验未覆盖 `closeLingerMs`。
- 后果：`closeLingerMs=0` 时，parked 连接若对端始终不回 FIN，将无限占用 fd 与 admitted 名额。
- 已采纳：启动校验拒绝 `<= 0`（与 `tickMs` / `idleTimeoutMs` 口径一致）。定为 `0 = 立即` 是错的——会抢在 FIN 之前拆掉还在排空的连接，把优雅关闭变成 RST。红证据（fault_injection case D）：`closeLingerMs=0` 与 `-1` 均被接受。

### P2-3：`closeConnection()` 的入队失败被静默吞掉 — **已修复 `8e0741f`**

- 位置：`closeConnection()` 原 L2016 忽略 `submitClose` 返回值。
- 后果：队列满（背压场景）时关闭请求丢失，连接要等到 idle 超时才被回收；调用方无从感知。
- 已改为返回 `bool`（true = 已入队），并把不可投递的关闭请求计入新增的 `closeRequestsDropped`。D-06 错误吞没。
- 红证据（backpressure case B）：队列已被 40000 个任务填满、`closeRequestsDropped +0`；修复后 `+1` 且返回 false。

### P2-4：统计口径不一致（帧 vs 字节）— **已修复 `7846036`**

- 位置：`submitSend` 任务内 L829-836：`isClosed` 分支计 `queueDrops++`（帧数），`closing` 分支计 `bytesDropped += size`（字节）。同一现象（关闭路径丢帧）落到两个不同量纲的计数器。
- 现已两边都记：`queueDrops` 回答"多少个应答没发出去"，`bytesDropped` 回答"对端少收了多少字节"。

### P2-5：部分计数器 RMW 用默认 seq_cst — **已修复 `107b606`**

- 位置：`enqueue()` 的 `queueDrops` / `pendingTasksTotal` / `pendingTasksDepth`、`SubReactor::loop()` 与 `acceptLoop()` 的 `reactorFailures`、`rejected`、`cleanup()` 的 depth 回退，共 7 处。
- 无语义错误，但与全文件"统计用 relaxed"的政策冲突（且 `enqueue` 是最热路径）。已统一，并在行内保留"为什么不 stronger"的理由引用。

## 待核实清单

1. [需核实：用户回调（MessageHandler/ConnStateHandler/CloseHandler）抛异常时杀死整个 reactor 是否符合预期契约] — 头文件注释已声明"must not throw"，loop 顶层 catch 会 `fail()` 停服，属文档化行为；若希望对单个坏回调免疫，需要在调用点包 try/catch。
2. [需核实：`drainWbufToSocket` 遇 EPIPE 返回 0 且 wbuf 非空时，是否存在 EPOLLOUT 反复触发但不消费的短自旋] — 正常路径下 EPIPE 前会先见 EPOLLERR/HUP 走 abort，该窗口极窄，静态分析无法排除。

## 复核结论（第一轮修复抽查）

- P2-5 队列上限：`pendingTaskCap_` 计算（L638-640）与 `enqueue` 守卫（L738）一致，backpressure 测试 9000/9000 通过，✅。
- P2-2 `pendingBytes`：12 处全部 `relaxed`，三段生命周期注释与实际路径（reserve L1992 / migrate L845 / release L958、L1216、L830、L841、L1146）一一对应，✅。
- 关闭协议：`closing`/`finSent` 分离、`drainInputThenClose` 最多等一轮、`doTick` 双截止，逻辑闭环，✅。
- P0-3/P0-4 发布顺序：`Running` 在所有 worker 创建之前发布（L1668），accept 就绪用 promise 同步，✅。

## 建议行动（按优先级）

## 验证结论（已完成）

仓库 HEAD `f176c1d`，工作树干净。每个缺陷单独一次提交，均为红→绿。

| 缺陷 | 红（修复前实测） | 绿（修复后实测） | 提交 |
|---|---|---|---|
| P1-1a 半关闭丢应答 | case E：`FIN, 0 of 229432 bytes` | `8194000 of 8194000 bytes`，FIN | `7a6f523` |
| P1-1b 停靠排空忙轮询 | `eofReads +35823 / +36903`（250ms） | `+0` | `a297d9b` |
| P1-4 关闭抢跑应答 | case F：8 次运行 4 次复现 | 8 次全部干净 | `f176c1d` |
| P2-1 unlimited 误报警 | 静态确定（`int` 环绕） | 提限路径不变（`1024 -> 65536`） | `e81c729` |
| P2-2 `closeLingerMs<=0` | case D：0 与 -1 都被接受 | 均被拒绝，1 仍可用 | `2006dd8` |
| P2-3 关闭请求静默丢失 | case B：`+0` 且无返回值 | `+1`，返回 false | `8e0741f` |
| P2-4 口径不一 | 帧/字节二选一 | 两个计数器都记 | `7846036` |
| P2-5 内存序不一致 | 7 处隐式 seq_cst | 统一 relaxed | `107b606` |

全量回归（WSL）：

```
cpplint           控制文件 2 findings（证明它能报错）；tcp-server-mt.h 0 findings
plain            close_semantics(默认/CS_REACTORS=1)、backpressure、fault_injection -> rc=0
asan+ubsan       同上 4 项 -> rc=0
tsan             同上 4 项 -> rc=0
Result: all configurations passed
```

## 原始建议行动（已全部执行）

1. P1-1：半关闭摘 EPOLLIN + 回归用例 —— `a297d9b`，用例 E。
2. P2-1、P2-2：配置/边界校验 —— `e81c729`、`2006dd8`（case D）。
3. P2-3：关闭请求丢失的可观测性 —— `8e0741f`，返回 `bool` + `closeRequestsDropped`（case B）。
4. P2-4、P2-5：统计口径与内存序风格统一 —— `7846036`、`107b606`。
5. 过程中反向发现并修复 P1-2 / P1-3 / P1-4（见上）。

## 仍然开放（未修）

- 待核实 1：用户回调（`MessageHandler` / `ConnStateHandler` / `CloseHandler`）抛异常会杀死整个
  reactor——这是文档化的契约（"must not throw"）。若希望单个坏回调免疫，需要在调用点包
  `try/catch`。已刻意不改：静默吞掉用户异常会掩盖应用层的 bug。
- 待核实 2：`drainWbufToSocket` 遇 EPIPE 且 wbuf 非空时的窄自旋窗口——正常路径下 EPIPE 之前会先
  见到 `EPOLLERR`/`EPOLLHUP` 走 abort，静态分析无法排除，也未构造出可靠复现。
- `MultiReactorServer::wait()` 仍是 200ms 轮询（非通知式），属已知设计取舍，非缺陷。

