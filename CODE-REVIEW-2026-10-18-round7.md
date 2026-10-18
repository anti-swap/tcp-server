# 第七轮：外部审查核实（`close` 与对端 FIN 落在同一次 `handleRead`）

本轮不是完整复审，只有一条：用户带来一张外部审查截图，断言 `drainInputThenClose()`
末尾的 `if (served == 0 || c->waitingOnReplies) parkClose(...)` 会在「对端的 request
与 FIN 被同一次 `handleRead` 读到」时提前发 FIN，把 handler 刚排队的应答丢掉。

结论：**断言成立，已修复（`864066b`）**，且截图对因果链的描述与代码逐行吻合。
补两点截图没有的：

1. 真正的根因不是"提前 FIN"，是**一个 bool 被两个主人共用，含义相反**；
2. 它和 N1（`cd52cf8`）是同一类失败的两个不同形态，N1 的修法覆盖不到这一条。

---

## 1. 代码事实

`drainInputThenClose()`（`tcp-server-mt.h:1694` 起）在 `handleRead()` 之后：

```cpp
const int served = handleRead(c);
...
if (served == 0 && !c->waitingOnReplies && c->pendingBytes > 0) {
    c->waitingOnReplies = true;               // 排一轮：让应答先落进 wbuf
    enqueue(parkClose...); return;
}
if (served == 0 || c->waitingOnReplies) {
    parkClose(c, "requested"); return;        // ← 截图标的那行
}
```

`waitingOnReplies` 有两个写入方，含义并不相同：

| 写入方 | 置位时的含义 | 那一轮的落点 |
|---|---|---|
| `drainInputThenClose()` | "我排了 `parkClose`，等应答落进 wbuf 后停车" | 停车 |
| `finishPeerClosed()` | "我排了自己的恢复任务，FIN 在应答落进 wbuf 之后" | 由 `finishPeerClosed` 判断：发 FIN，或直接收尾 |

第 1750 行把第二种也读成了"这一轮已经用掉了"。于是：

1. 应用线程调 `closeConnection()` → 队列里只有 `submitClose` 一个任务；
2. 该任务进 `drainInputThenClose` → `handleRead` 一次读完 request，随后 `recv` 返回 0；
3. `handleRead` 内同步调用 `finishPeerClosed`：`wbuf` 空、`pendingBytes > 0`
   → 置 `waitingOnReplies`，排入恢复任务 → 返回；`handleRead` 返回 `served = 1`；
4. 回到 `drainInputThenClose`：`served > 0`，`rbuf` 已空（不进 N1 那轮重排），
   `served == 0` 为假但 `waitingOnReplies` 为真 → **立刻 `parkClose`**；
5. `parkClose` 第一件事就是 `closing = true`，此时 `wbuf` 仍为空（应答还排在队列里没执行）
   → `tryBeginWriteShutdown()` 立刻 `shutdown(SHUT_WR)`；
6. 之后 echo 任务执行 → `submitSend` 见 `closing` → 记账丢弃：`bytesDropped += 40`、
   `queueDrops += 1`、`framesSent` 不动。

对端的收成：一个整齐的 FIN，少一帧。这正是 `drainInputThenClose()` 顶部注释写明
要防的那种结局（"the peer receives an orderly FIN over requests nobody ever answered"）。

## 2. 复现手段

`verify/evidence/probe_close_vs_fin.cpp`。窗口本身是竞态：同样三个事件换个顺序就
什么都不丢，所以探针用**闸门**把竞态消掉——拿第二条连接的处理函数把 reactor 卡住
（`cv.wait`），此时：

- subject 的 request + FIN 一定还在内核缓冲里，没人读；
- 应用线程调 `closeConnection()`，队列里只有这一个任务；
- 放开闸门后，`processPending()` 先跑队列（loop 顺序：事件循环 → `processPending`），
  于是 `drainInputThenClose` 的 `handleRead` 必然同时消费 request 与 FIN。

三个用例：

| 用例 | 构造 | 问什么 |
|---|---|---|
| A | subject 发 request 后 `shutdown(SHUT_WR)`，闸门卡住时请求关闭 | 应答到了对端没有 |
| B | 完全同构，只少一次 `shutdown` | 差别确实来自 FIN，而不是闸门 |
| C | 用 subject 自己的 handler 当闸门（先 `sendTo` 再卡住） | 改动后的分支在 `served == 0` 下（常见顺序）是否照样收尾 |

C 是**回归守卫**，不是修复目标：它走的是同一个"交回"分支，但那一刻应答已经落进
`wbuf`，修复前后都应该是绿的。

## 3. 红 → 绿

同一个二进制分别编 `HEAD` 与工作树：

```
===== BEFORE (HEAD, waitingOnReplies) =====
A. close while request + FIN are unread
   closeConnection() = true, handler answered 1, peer received 0 bytes / 0 frame(s)
   stats: framesRecv 2 framesSent 0 bytesDropped 40 queueDrops 1 closed 1 aborted 0
B. control: identical, no FIN
   peer received 40 bytes / 1 frame(s), bytesDropped 0 queueDrops 0
C. 20 iteration(s), accepted in 20, answered in 20, bytesDropped 0 queueDrops 0
   [FAIL] A: 0 of 1 frame(s)      [FAIL] A drops: bytesDropped 40, queueDrops 1
   [PASS] B                       [PASS] C
before run rc=1

===== AFTER (working tree, deferredRound) =====
A. close while request + FIN are unread
   closeConnection() = true, handler answered 1, peer received 40 bytes / 1 frame(s)
   stats: framesRecv 2 framesSent 1 bytesDropped 0 queueDrops 0 closed 1 aborted 0
B. control: identical, no FIN
   peer received 40 bytes / 1 frame(s), bytesDropped 0 queueDrops 0
C. 20 iteration(s), accepted in 20, answered in 20, bytesDropped 0 queueDrops 0
   [PASS] A                       [PASS] A drops: nothing
   [PASS] B                       [PASS] C
after run rc=0
```

修复前连跑 5 次都是同一组数字（`bytesDropped 40 queueDrops 1 closed 1`），
修复后连跑 5 次也是（`framesSent 1`、零丢弃）——闸门确实把竞态变成了确定性。

**一个测量陷阱**：第一版在 `recv` 收到 FIN 后立刻取计数器，两次跑出两个不同结论
（一次 `bytesDropped 40`，一次 `0`）：FIN 到达对端时，echo 任务还在队列里，丢弃
尚未记账。所以取数前先等 `closed` 动——关闭那一轮一定排在丢弃之后。

## 4. 处置（`864066b`）

`Conn::waitingOnReplies` → `Conn::deferredRound`，把"谁欠这一轮"编进类型：

```cpp
enum class DeferredRound : uint8_t { None = 0, Close, PeerClosed };
DeferredRound deferredRound = DeferredRound::None;
```

`drainInputThenClose()` 里，在原来那行停车之前加一条：

```cpp
// 对端的 FIN 与 request 在同一次 read 里被读到，finishPeerClosed() 已经认领了这一轮。
// 在这里停车会置 closing，而 wbuf 仍为空 → FIN 立刻发出，正好盖住这一轮要保护的应答。
if (c->deferredRound == Conn::DeferredRound::PeerClosed) return;

if (served == 0 || c->deferredRound == Conn::DeferredRound::Close) {
    parkClose(c, "requested"); return;
}
```

**为什么"交回"就安全**：`finishPeerClosed()` 只有两种出口——排入恢复任务，或者
（入队失败时）直接 `teardownClosed()`。不存在"这一轮有人欠着、但没东西会回来还"
的状态。收尾仍然优雅，FIN 仍然在所有欠账之后，只是由**看得见还欠多少**的一方决定。

`finishPeerClosed()` 与 `drainInputThenClose()` 自己的两处置位同步改用枚举，
`deferredRound == None` 就是原先 `!waitingOnReplies` 的"至多一轮"不变式。

## 5. 与 N1 的关系

N1（`cd52cf8`）修的是同一类失败的另一种形态：

| | 触发 | 现场 | 修法 |
|---|---|---|---|
| N1 | `served > 0` 且 **`rbuf` 还有残留**（读预算耗尽） | 停车会拒掉后面那些帧的应答 | 用 `closeInputBytes` 记账，重排一轮 |
| R7-1 | `served > 0`、**`rbuf` 已空**、应答还在队列里、FIN 同批到达 | 停车会拒掉已经排好队的应答 | 交回给欠这一轮的 `finishPeerClosed` |

两条的共同点：判断"还能不能停车"时，`served > 0` 本身说明**有东西欠着**，
而原代码只把 `served == 0` 当欠账信号。

## 6. 为什么六个套件一直是绿的

套件里确实有从处理函数之外调 `closeConnection()` 的地方（`close_semantics` 两处、
`ping_pong`、`send_results`），但**每一处都先等到请求已被读走**——等 `bytesRecv`
达标，或 `sleep 50ms`。那正是这个窗口不可能命中的顺序。命中需要：

- `closeConnection()` 从处理函数之外发起（业务侧超时踢人、会话收尾、优雅下线）；且
- 对端的最后一次请求尚未被 reactor 读走；且
- 对端在同一次 read 里半关闭。

从 handler 内部请求关闭不会中招：echo 任务排在 close 任务之前，N1 / 第五轮建立的
顺序救了它。

## 7. 回归

| 检查 | 结果 |
|---|---|
| `verify/run-in-wsl.sh`（plain，7 组） | 7/7 rc=0 |
| `ENABLE_ASAN=1 ENABLE_TSAN=1`（plain / asan+ubsan / tsan × 7 组） | 21 配置全 rc=0 |
| `cpplint tcp-server-mt.h` | 0 findings |
| 主机侧 `verify/build.sh` | 19 TU 无警告 |
| `probe_close_vs_fin`（修复后连跑 5 次） | 5 × rc=0 |

## 8. 待核实

- `[需核实: 真内核上的 hup/err]` —— 与第六轮同一条未关闭项：本轮未触及。
- `[需核实: 多 reactor 下 C 用例的命中率]` —— C 用 `numReactors = 1` 跑；
  多 reactor 不改变这两个函数的控制流（都只在 owner 线程上跑），但本轮未实测。
