# 第八轮：外部清单四条核实

外部清单给了四条并附了处置建议。本轮按建议逐条核实、逐条处置：

| 条目 | 级别 | 结论 | 处置 | 提交 |
|---|---|---|---|---|
| R8-1 `closeInputBytes` 让 peer 可延长 close | P2 必修 | **成立**，且比清单说的更严重 | 已修（不是一行） | `bc22b63` |
| R8-2 `stateHandler` 抛异常时 `accepted`+`rejected` | P3 建议修 | **成立** | 已修 | `d8692ba` |
| R8-3 N1 变体：`handleWrite` 抢在 `processPending` 前 | P3 可选 | **不成立**（今天没有可达路径） | 写下判据 | `a875bec` |
| R8-4 `unregisterConn` 清 `closing` / `cleanup` 未回滚 | P4 可选 | **成立但惰性**（无读取方） | 写下理由 | `a875bec` |

三条核实下来有一个共同点：真正的问题和被列出来的名字都不完全一样。下面按条写。

---

## 1. R8-1（P2）：close 预算是以「帧」计价的

### 代码事实

`drainInputThenClose()` 被允许为了排空对端已积压的请求多跑几轮（`handleRead` 每次最多
256 帧 / 64 KiB）。轮数由 `Conn::closeInputBytes` 约束，它在首次进入时量一次：

```cpp
if (c->closeInputBytes < 0)
    c->closeInputBytes = static_cast<int64_t>(c->rbuf.readable());
...
if (served > 0)
    c->closeInputBytes -= static_cast<int64_t>(served) * FRAME_HEADER_SIZE;   // ← 八字节一帧
```

一帧按 8 字节（`FRAME_HEADER_SIZE`）扣账，跟它实际多大无关。所以这不是一个字节预算，
而是一个「`backlog / 8` 帧」的帧预算，而每一帧可以宽到 `MAX_FRAME_PAYLOAD`（64 KiB）。
一个满 `rbuf` 的积压就换来约 8000 帧的额度。

而且没有第二条兜底线：此时连接还没停车，`closeStartedAtMs` 没被盖上 → 没有 linger 期限；
每次 `recv()` 都刷新 `lastRecvMs` → idle 也永远不算到期。清单说「peer 可无限延长 close」，
在这台机器上的实测是：为了 16 帧（约 131 KB）的积压，peer 在应用要求挂断之后还能被认真
地服务掉**上两千帧**。

### 第一次改法是错的（值得记下来）

最直接的想法是「按实际消费的字节扣账」，用 rbuf 前后差值：`before - after`。
在「边收边发」的稳态下这个值恒为 0 —— peer 灌进来的字节被**同一次 read** 消费掉了，
`before` 和 `after` 都维持在高位，差额接近零，账就永远扣不完。当时看着像有改善
（同一场景被应答的帧从 210 降到 63），其实没有收敛。

正确写法是：`消费掉的字节 = 之前缓冲的 + 这一次 read 拉进来的 − 剩下的`。
这是 FIFO 的，积压从队首先被扣掉，之后 peer 补进来的东西对着残余额度自己付账。
`handleRead()` 因此多报一个数：`Conn::drainReadBytes`（由唯一的读者先清零）。

### 不可消除的那一部分

修完之后不是零，把它写成零的注释是没用的。`loop()` 在看自己任务队列之前，先跑
`readHead_` 续跑，最多 64 次 `handleRead`，每次再读 64 KiB —— 也就是一次循环迭代能在
「挂断请求已经躺在队列里」的情况下，认真处理 ~4 MiB 的新输入。这是单线程共用的固有代价，
只有在上面的「多跑几轮」不断给它新的迭代时才会变成无界。

所以正确的判据不是「挂断之后不再服务任何东西」，而是「**越推越多的 expressed 是否能换来越多的服务**」。

### 探针：`verify/evidence/probe_close_budget.cpp`

同一个连接、同样的 16 帧积压，之后 peer 洪泛 1200 / 4800 帧，两次。每一帧用不同的填充字节，
让 handler 自己数「挂断之后到达、且应答被接收」的帧——服务器侧计数器做不到这件事：
`framesRecv` 把 linger 期间读了就丢的帧也算进去，`bytesSent` 里有一大截是挂断时本就欠着的
应答（那些本来就该排在 FIN 前面发出去）。

```
BEFORE  小洪泛：推 1200，应答 1194，拒 6；FIN 43 ms
        大洪泛：推 2164，应答 2026，拒 138；FIN 65 ms
        [FAIL] 四倍的洪泛没换来四倍的服务（2026 对 1194，上限 1258）

AFTER   小洪泛：推  648，应答 511，拒 110；FIN 14 ms
        大洪泛：推  600，应答 511，拒  56；FIN 14 ms
        [PASS] 511 对 511
```

511 不是给断言挑的整数：64 次读续跑 × 64 KiB = 4 MiB，4 MiB 的 8 KiB 帧 = 511 帧。
连接被欠的量是「一次循环迭代所能读到的东西」，与 peer 推多少无关——所以它也是确定的
（连跑三次：511、511、511）。

### 探针设计里踩过的三个坑

留档，免得下次重新踩：

1. **帧比读预算还大时，rbuf 里永远只有半帧**，`served` 恒为 0，一轮都不会延期——
   看起来「没问题」，其实是场景根本没进入被测的那条路径。
2. **flood 太快**时，peer 在关闭任务被看到之前就把 3 MB 灌进 socket，`loop()` 顶部那一轮
   读突发会把它全部处理掉（当时实测 handled 400/400），测出来的其实是队列延迟，不是预算。
3. **服务器计数器无法归因**：欠账排空和 linger 期的丢弃都混在里面。最后靠填充字节标记解决。

---

## 2. R8-2（P3）：一条连接同时被计进 `accepted` 和 `rejected`

`submitNewConnection()` 的登记任务结尾：

```cpp
server_->statsRef().accepted.fetch_add(1, relaxed);
auto& sh = server_->stateHandlerRef();
if (sh) sh(id, c->peer, true);
} catch (...) {
    server_->statsRef().rejected.fetch_add(1, relaxed);
}
```

到这里连接已经在两张表里、已经拿到 epoll 注册，不会因为应用的 `ConnStateHandler` 抛了就撤销。
实测（`verify/evidence/probe_state_throw.cpp`）：

```
  callback ran for 1 open(s)
  connectionCount() = 1 -- it is connected
  accepted = 1, rejected = 1, closed = 1
```

一条还活着的连接，同时被说成「收下了」和「拒掉了」，之后 `closed` 又计它一次。
有意思的不是数字错，而是让人信哪一个：`accepted - closed` 能对上现实是这个文件里别的改动
一直维护的东西（正因为如此 `accepted` 才被挪进这个任务里），所以错的是 `rejected`——而它正
是运维用来看「是不是在赶人」的那个计数器。改法是给 state 回调单独一个 try/catch，什么都不计。
没有「应用回调抛了」的计数器可加：`ServerStats` 里没有一个放它的位置不会变成对本服务器的某种声明，
而异常本身已经回到写它的人手上了。

**顺带确认的一条**：从 *close 侧* 的回调抛异常是另一回事，而且短得多——它们在
`flushCloseCallbacks()` 里跑，那是 `noexcept` 且外面没有任何保护，所以这种契约违规由
`std::terminate` 强制执行，没有留下的记账可以被误读。探针只在 open 转换上抛，并写明了原因。

```
AFTER  accepted = 1, rejected = 0, closed = 1
       [PASS] accepted counts it / rejected does not count it too / closes once
```

---

## 3. R8-3（P3）：N1 变体 —— `handleWrite` 抢在 `processPending` 之前

清单担心的是：`loop()` 一次迭代里先是事件（`handleRead` 再 `handleWrite`），最后才
`processPending()`，所以 handler 排下的应答还没落进 wbuf 时，同一 `fd` 的 `EPOLLOUT` 已经在事件
阶段先跑完了 `handleWrite()`，它会看着一个空的 wbuf 决定是否拆连接。

**结论：今天没有可达路径。** 判据写进了 `handleWrite` 的决策点：

- 应答任务只从三处进队列：handler（跑在读续跑或事件阶段，都在 `processPending()` 之前）、
  另一个任务、以及应用线程调 `sendTo()`。
- 置 `closing` 也只有三处：任务（`submitClose` / `drainInputThenClose` 的那一轮）、
  `doTick()` 的扫描、以及本分支自己调 `parkClose()`。于是：
  - 对前者：排在它之前的应答在同一个 FIFO 批次里排在它前面，`processPending()` 顺序执行，
    所以它置位时那些应答已经落地；
  - 对 `doTick()`：扫描迭代的 `processPending()` 之后跑，handler 产出的任务不可能还压着；
  - `finishPeerClosed()` 那条路走的是 N1 加的同一个 `pendingBytes` 守卫。

残余情况是有的、也是已经记账的：在队列排空与扫描之间从别的任务或别的线程进来的提交，确实
可能被 FIN 盖过去 —— 这正是 `Ok` 的字面含义（「已入队，不等于已送达；shutdown 或断连可以丢
掉未完成的工作」），由 `submitSend` 拒进 `queueDrops` / `bytesDropped`。在这里加守卫既不会让
这笔账更好看，还会让每个停靠 socket 的 `EPOLLOUT` 都多绕一圈。

---

## 4. R8-4（P4）：`unregisterConn` 清 `closing`

`unregisterConn()` 把 `closing` 清了，但 `finSent` / `peerClosed` / `deferredRound` /
`closeInputBytes` 都没动，读起来像半份回滚。它是**惰性**的：所有 `closing` 的读取方要么走
`conns_`（连接还在），要么先看 `isClosed`；而两个调用方到达这一行时 `isClosed` 都已经是 true
—— `teardownClosed()` 在 unregister 之前就置了位，`discardRejectedConn()` 则是边走边删。
所以清它不会「解除停车」任何东西，也不该有人依赖它做过这件事。理由写在代码里，免得以后有人
把它读成可以依赖。

`cleanup()` 那半边也顺手核对了：它对剩余连接走 `closeConn(..., "server shutdown", false)`，
计数 `closed` + `aborted`，再 `discardPendingOutput()` 把wbuf 里的欠账核销，
`pendingTasksDepth` 也先减了。第六轮 R6-4 之后不再有幽灵连接（未 accepted 的连接不再被收尾），
所以这里没有需要回滚的东西。

---

## 5. 本轮新增证据

- `verify/evidence/probe_close_budget.cpp` —— R8-1，两量级洪泛对照 + 按填充字节归因
- `verify/evidence/probe_state_throw.cpp` —— R8-2，open 回调抛异常时的三个计数器

---

## 6. 回归

| 检查 | 结果 |
|---|---|
| `verify/run-in-wsl.sh`（plain，7 组） | 7/7 rc=0 |
| `ENABLE_ASAN=1 ENABLE_TSAN=1`（plain / asan+ubsan / tsan × 7 组） | 21 配置全 rc=0 |
| `cpplint tcp-server-mt.h` | 0 findings |
| 主机侧 `verify/build.sh` | 21 TU 无警告 |
| `probe_close_budget`（修复后连跑 3 次） | 3 × rc=0，回答帧数恒为 511 |
| `probe_state_throw` | rc=0 |

---

## 7. 待核实

- `[需核实: 真内核上的 hup/err]` —— 与第六、七轮同一条未关闭项，本轮未触及。
- `[需核实: 多 reactor 下 R8-1 的 511 是否换数]` —— 511 来自单个 reactor 的一次循环迭代；
  多 reactor 不改变 `drainInputThenClose` 的控制流（只在 owner 线程跑），但本轮只实测了
  `numReactors = 1`。
- `[需核实: R8-3 残余情形是否应当有专属计数]` —— 现在它混在 `queueDrops` /
  `bytesDropped` 里，与「真正的关闭分支」同计数。按 `Ok` 的契约这是同一件事，若以后要分开看
  需要新增计数器，本轮按「可选」没做。
