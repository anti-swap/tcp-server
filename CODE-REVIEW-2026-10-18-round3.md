# tcp-server-mt.h 代码审查报告（第三轮）

- 项目：`C:\Users\root\WorkBuddy\Claw\tcp-server`
- 审查时间：2026-10-18
- 审查模式：full（完整模式）
- 基线：`0c9d363 docs(review,verify): close out the second review round`
- 性质：**第三轮复审**。第一轮（CODE-REVIEW.md）P0-1~P2-6、第二轮（CODE-REVIEW-2026-10-18.md）P1-1a/1b/1-4、P2-1~P2-5 均已修复并通过 plain / ASan+UBSan / TSan / cpplint。本轮在 2152 行的当前版本上重新扫描，只报**仍然存在或新发现**的问题。
- **修复状态（全部完成）：P1 与 P2-6/7/8/9/10 已由 `18749fe..a8235dd` 共 10 笔提交修复**；待核实 #1 由维护者裁决关闭；待核实 #2 / #3 由 `1f4f899`、`f27cfc1`、`7f5247a` 三笔关闭。至此第三轮清单归零，只剩上一轮有意保留的两项设计取舍。每笔缺陷独立提交，可验证的均走红→绿，验证矩阵见文末。

## 摘要

| 项目 | 结果 |
|---|---|
| 环境 | 单头文件 `tcp-server-mt.h`（2152 行，LF）+ `verify/` 4 个测试 + `verify/stub/` 编译期桩；无依赖清单文件 |
| 规范匹配 | 无 `.coding-standards.md` → 启用内置宽松规范；另有项目级 `CPPLINT.cfg`（linelength=100）正在生效 |
| 提交历史 | 最近一系列提交每次缺陷都配了独立的红色测试提交，无多次编辑压缩成一个 commit 的情况 |
| 测试闭环 | **正常**。第二轮 6 个缺陷全部为红→绿两笔提交（`test(verify):` 先行、`fix(...)` 随后），有 WSL 执行记录，无"形式闭环" |
| **缺陷统计** | **P0 = 0，P1 = 1，P2 = 5，待核实 = 3 —— 全部已修复 / 关闭（13 笔提交），开放项仅剩上一轮的两项设计取舍** |
| 本轮特殊性 | 有 1 项**初始推断被实测推翻并主动撤回**，见第五节 |

> 这一轮刻意对每个推断都做了运行时验证而不是停在静态判断上。结果是 5 个候选缺陷里有 1 个是**我臆断的假警报**（EOF 后重复读），已删除。剩下的都带可复现的数字。

---

## 一、P1：`framesSent` 会把从未发出的帧永久计为"已发送" — **已修复 `0229f00`**

- 位置：`tcp-server-mt.h:880`（submitSend 写入 wbuf 后）、`:1232`（PING/PONG 写入 wbuf 后）计 `framesSent`；`:984`（`drainWbufToSocket` 的 sendmsg 成功）计 `bytesSent`。
- 缺陷模式：D-07 隐式假设（两个名字对称的计数器被默认当成了同一计量点）+ 第二轮 P2-4 的**同源残留**。

### 机制

两个对外最主要的"发出去多少"指标，取的时刻完全不同：

| 计数器 | 自增时刻 | 会不会回头减 |
|---|---|---|
| `framesSent` | 帧**进入 wbuf**（用户态缓冲）时 | **不会** |
| `bytesSent` | 字节**真正离开 socket**（sendmsg 返回）时 | 不适用 |

一帧一旦被 `framesSent` 计上，就再也没有任何路径把它摘下来：
`teardownClosed → discardPendingOutput`（L1292）只把 wbuf 里剩下的字节计进 `bytesDropped`，**不动 `framesSent`**；
`framesDroppedOnClose` 只在"PING 撞上 closing"那一处自增（L1227），名字很像扣分处，其实和这个漏斗毫无关系。

### 实测（本机，WSL）

构造：单 reactor，`idleTimeoutMs=300 / closeLingerMs=200 / tickMs=20`，客户端发一个请求后**一个字节也不读**，handler 回 255 帧 × 32800 字节；linger 到期后 `doTick` 以 `closeConn(..., false)` 收尾，`discardPendingOutput` 丢弃 wbuf 余额。

```
repliesAccepted = 255
framesSent      = 255        ← 对外报告"发出了 255 帧"
bytesSent       = 3145728    ← 实际离网 95 整帧 + 29,728 字节半帧（255 − 95 ≈ 160 帧未达）
bytesDropped    = 5212152    ← 约 159 帧从未到达对端
bytesSent/framesSent = 12336.2   而每帧线上实际是 32800 字节
```

**对外的一致性欺骗**：`framesSent - (bytesSent / 每帧字节)` 这个显而易见的校验式没法成立，因为那个除数本身并不存在；更糟的是它**恰恰在最需要它的时刻失真**——丢帧、abort、背压。上一轮修掉的 P2-4 是"关闭路径两种货币不一致"，本题是同一个根，但更硬：那个还能靠说明分工来文档化，这个没有任何正确读法。

### 建议与实际修复

二选一，推荐前者——实际落地的就是它：
1. **已实施**：把 `framesSent` 的计数点下移到 `drainWbufToSocket`，在帧的最后一个字节离开 wbuf 进内核时计数，与 `bytesSent` 同点。支撑机制：`Conn::sendFrameRemain`（头部帧剩余字节；wbuf 只收整帧，所以头部要么在某一帧内部、要么恰在边界上）+ `RingBuffer::peekAt`（从可读区任意偏移读帧头）。`discardPendingOutput` 同步清零 `sendFrameRemain`。
2. 若必须保留"已接纳"语义，则改名为 `framesAccepted` 并写明关系——未采用。

红→绿验证（`close_semantics` case G，构造即上文"客户端不读 + `closeLingerMs` abort"）：

```
红 18749fe：framesSent=511  bytesSent=3145728  bytesDropped=30347256  → [FAIL]
绿 0229f00：framesSent=47   bytesSent=3145728  bytesDropped=30347256  → [PASS]
```

绿灯数字即不变量本身：`framesSent == bytesSent / 每帧线上字节数`（3145728 / 65544 = 47，恰为 47 整帧 + 65,160 字节半帧），与内核缓冲吸收多少无关，任意机器上都精确成立。

- 置信度：**高**（已复现，红→绿数字稳定）。

---

## 二、P2

### P2-6（遗留未决）：`INDUSTRIAL_TCP_LINGER_S` 是死宏 — **已修复 `c576062`（删除）**

- 位置：`tcp-server-mt.h:51-59`
- 该宏有定义、有一段 7 行注释解释它的作用（压缩 TIME_WAIT、避免占满 fd），但**全仓库没有任何一处使用**：

```
grep -rn INDUSTRIAL_TCP_LINGER_S .   →  仅 L57、L58 两处（#ifndef / #define）
```

- 这还是一个**闭环漏洞**：第一轮 `CODE-REVIEW.md:270` 已经写明"配置项叫 `closeLingerMs`…`INDUSTRIAL_TCP_LINGER_S` 宏已定义但当前未使用，留到 P2-3 一并处理"，但第二轮的 P2-3（`closeConnection()` 吞掉入队失败）是另一件事，这个承诺从来没兑现。缺陷模式：**D-03 测试盲区 / 遗留项丢失**。
- 建议：三选一——删掉宏并把注释改成对 `SO_LINGER` 只用于 abort 的说明；或真正用它（在 accept 路径 `setsockopt(SO_LINGER, {1, INDUSTRIAL_TCP_LINGER_S})`）；或至少把"未使用"写进注释，避免下一个人再当作旋钮。
- **实际处理**：删除（选项一）。给它接线会把正常关闭的语义改成 SO_LINGER 主导，与本文件"默认 FIN、仅 abort 用 RST"的整个设计相抵触；`SO_REUSEADDR` 已覆盖重绑场景；abort 语义的说明在 `unregisterConn` 处本来就完整。
- 置信度：高（静态可判定）。

### P2-7：`handleRead` EOF 分支里的 `framesLeft <= 0` 分支不可达 — **已修复 `a32fc3d`（换成 assert）**

- 位置：`tcp-server-mt.h:1036-1046`（`n == 0` 分支内的 `if (framesLeft <= 0) { scheduleRead(c); return; }`，连同 6 行注释）
- 静态论证：`handleRead` 的循环体顺序是 `parseFrames` → `if (framesLeft <= 0 || bytesLeft <= 0) 退出` → `recv`。因此**任何一次 `recv` 被调用时，"rbuf 里已缓冲的完整帧数 < framesLeft"这个条件必然成立**（否则上一轮的 `parseFrames` 就会把预算打光并在检查点退出）。而 `recv` 返回 0 意味着没有新字节进来，所以紧随其后的 `parseFrames` 不可能再把 `framesLeft` 打到 0。
- 实测佐证（4 种突发规模，单 reactor，发完立即 `shutdown(SHUT_WR)`，读干）：

```
burst=1     framesRecv=1     eofReads=1   replyBytes=72
burst=8     framesRecv=8     eofReads=1   replyBytes=576
burst=300   framesRecv=300   eofReads=1   replyBytes=21600
burst=2000  framesRecv=2000  eofReads=1   replyBytes=144000
```

  若该分支可达，一次 FIN 会被读多次，`eofReads` 会 > 1。四条都是 1，与"不可达"一致。
- 建议：把这段换成 `assert(framesLeft > 0);`。附带好处：`eofReads` 作为 P1-1b 引入的忙轮询探针，其语义（"每条连接至多算一次 FIN 唤醒"）会从注释约定变成编译期事实。
- **实际处理**：即此。死代码连同 6 行"解释"删除，论证（为何每个能到 `recv()` 的路径都已保证缓冲完整帧数 < 预算）连同实测数字（burst 1..2000 下 `eofReads` 恒为 1）写进 assert 上方的注释。全套件回归通过，断言未触发。
- 置信度：中高（静态论证完整 + 实测一致；但"不可达"无法被有限次运行完全证明，故建议用 assert 而不是直接删）。

### P2-8：`submitClose` 里两组重复且互相打架的注释 — **已修复 `b19fef8`**

- 位置：`tcp-server-mt.h:896-908`

```cpp
// Copy out and reset the map handle before closing: ...
// Default (graceful): this is an application-level "we are done",
// so the reply queued just before it must still reach the peer.     ← 第一段
std::shared_ptr<Conn> c = it->second;
Conn* raw = c.get();
c.reset();
// Default (graceful): this is an application-level "we are done",
// so the reply queued just before it must still reach the peer --   ← 第二段，同一句话
// and so must answers to requests the peer already sent but this
// reactor has not read yet.
drainInputThenClose(raw);
```

- 缺陷模式：**D-05 复制粘贴变异**。P1-4 修改时第二段被插入，第一段没有删。二者说的是同一件事，但第二段的结论才是对的（多了"尚未读取的请求也必须先回答"），留着这一段会让读者拿着旧语义去理解新逻辑。
- 建议：删掉第一段，保留第二段。
- 置信度：高。

### P2-9：配置校验的两处缺口 — **已修复 `da11778`**

- 位置：`tcp-server-mt.h:482`（`backlog` 无校验）、`:1700-1703`（`start()` 的校验清单）
- 1. **`backlog` 未校验**：唯一一次使用是 `::listen(fd, cfg_.backlog)`（L1684）。`backlog=0` 会被原样传给内核（Linux 只在超过 `somaxconn` 时下调），得到一个长度为 0 的 accept 队列，表现为"高并发时莫名其妙拒连"，且和 `maxConnections`/`RLIMIT_NOFILE` 那套主动 inspecting 的告警机制完全脱节。负数会被隐式转成巨大的 unsigned 再被 clamp，反而无害。
   - 建议：与 `tickMs` 等并列加一条 `cfg_.backlog <= 0` 拒绝。
   - `[需核实：listen(fd, backlog=0) 在目标内核上的确切行为——是不是真的得到 0 长度队列，随内核版本可能有差异]`
- 2. **tick 粒度与两个超时没有约束关系**：`tickMs` 只要求 > 0，但 `doTick()` 的执行间隔就是 `idleTimeoutMs` 和 `closeLingerMs` 的实际分辨率。`tickMs=5000, closeLingerMs=200` 意味着承诺 200ms 的宽限期实际可能是 5s 才到期——`closeLingerMs` 的第二轮修复（拒绝 <= 0）保证了它"最终会到期"，但没有保证它"大致按时到期"。
   - 建议：`start()` 里当 `tickMs > closeLingerMs` 或 `tickMs > idleTimeoutMs` 时给一条 stderr 提示（不必拒绝，某些场景大 tick 是刻意的）。
- 置信度：高（静态）+ 中（`backlog=0` 的内核行为）。

### P2-10：内存序风格残留 2 处 — **已修复 `a8235dd`**

- 位置：`:263 / :268`（`Admission` 的 `++count` 与 `--count`，默认 seq_cst）；`:1236`（`parseFrames` 里 `c->isClosed.load()` 未指定 order，其余 9 处都是 `std::memory_order_acquire`）
- 本轮复核发现：`:263/:268` 是计数 RMW，与 `pendingBytes` 注释里声明的 relaxed 策略同源，属于第二轮 P2-5 统一时的遗漏（那次只扫了 `fetch_add/fetch_sub`，没扫运算符形式）；`:1236` 是 `acquire` 语义但写成了默认 seq_cst，与九处兄弟写法不一致，会误导读者以为这里"刻意不同"。
- 建议：分别补 `std::memory_order_relaxed` 与 `std::memory_order_acquire`。
- 置信度：高。

---

## 三、待核实清单

| # | 内容 | 为什么不能静态判定 |
|---|---|---|
| 1 | **已裁决关闭（维护者）**：`lastRecvMs` 按字面意思就是"最后收到的时间"，实现与命名一致，发送侧不计活动是命名本身宣称的语义，不构成缺陷。后续若产品需要"双向静默才算 idle"，再引入独立字段，不覆写这个。 | — |
| 2 | **已修复（`1f4f899` + `f27cfc1`）**：`doTick()` 的 range-for 里调用 `closeConn`，而 `teardownClosed` 会从正在被迭代的 `conns_` 里 erase。安全性依赖"仍在 `conns_` 里的连接若 `finSent` 则必 `closing`"这条既不注释也不检查的不变量。 | 不变量跨 5 个函数，静态难以证否。用注入证明危险为真（`check_doTick_invariant.sh`：before = ASan heap-use-after-free，after = rc=0），并把到期关闭移到 sweep 之外执行——erase 在活迭代器下结构上不可能发生，强于原建议的 assert（NDEBUG 下会被编译掉）。详见第六节。 |
| 3 | **已修复（`7f5247a`）**：原判断"`drainWbufToSocket` 的 EPIPE 分支仅保留 EPOLLOUT 挂载"漏了更根本的东西——**该函数的两种零结局共用返回值 0**：wbuf 排空、以及 EPIPE。调用方能分清全靠"EPIPE 在循环内返回，故 wbuf 必非空"这个没人写下来的耦合。 | 「建议加一行 `disarmWrite`」是错的：`disarmWrite` 在 `wbuf` 非空时直接 return，那样改是空操作；若强行放开，它又会把掩码改回 `EPOLLIN`，在 P1-1b 修过的 peer-FIN 停靠场景里把读自旋放回来。改为把结局命名为 `DrainOutcome::{Drained,Stalled,Dead,Error}`，`Dead` 在两处调用点作终态处理。详见第六节。 |

## 四、上一轮遗留的两项（本轮维持开放，未重复计入）

1. **用户回调抛异常会杀死整个 reactor** —— 文档化契约（`parseFrames` → handler → `handleRead` → `loop()` 的 `catch(...)` → `fail()`）。改为静默吞掉会掩盖应用层 bug，故不建议改；若要改，应落成"每连接隔离 + 计数上报"的完整设计，而不是 try-catch 了事。
2. **`wait()` 是 200ms 轮询**（`:1880`）—— 已知设计取舍，非缺陷。

## 五、本轮主动撤回的一个推断（记录以防复发）

初读时我判断：`n == 0` 分支里的 `scheduleRead`（L1044）会让 reactor 在已经读到 EOF 的 socket 上**反复调用 `recv`**，每次返回 0 并给 `eofReads` 加一，`eofReads` 作为 P1-1b 引入的忙轮询探针会失真。

**这个判断是错的。** 上面的静态论证 + 4 组实测（`eofReads` 恒为 1）都表明该路径不可达；真正的结论是 P2-7（分支是死代码）。如果直接写进报告，会得到一个"听起来很有道理的危害原语"和一个错误的 lineno 指向——所以这一节留在这里，作为"先测后报"的成本收益记录。

## 六、修复记录（原"建议行动"，已完成）

按约定顺序一次一缺陷、红→绿、单独提交，`18749fe..a8235dd` 共 10 笔：

| 缺陷 | 测试提交 | 修复提交 | 关键红→绿证据 |
|---|---|---|---|
| **P1** framesSent 计量点 | `18749fe` | `0229f00` | case G：framesSent 511→47，`== bytesSent/65544` 精确成立 |
| P2-6 死宏 | — | `c576062` | grep 归零 + compile_check 通过（纯删除） |
| P2-8 重复注释 | — | `b19fef8` | 纯注释，无行为变化 |
| P2-7 不可达分支 | — | `a32fc3d` | case E 回归通过、断言未触发（eofReads 探针由 case E 持续把守） |
| P2-9 backlog 校验 | `925d6f8` | `da11778` | case D：0/-1 被接受 → 均被拒绝；tick 告警在套件内真触发且不误报 |
| P2-10 内存序 | — | `a8235dd` | TSan 全量回归通过（无新增报告） |
| 待核实 #2 doTick 迭代 erase | `1f4f899` | `f27cfc1` | 注入不变量破坏：before = ASan heap-use-after-free，after = rc=0（`accepted=24 closed=24 stillOpen=0`） |
| 待核实 #3 drain 结局共用 0 | — | `7f5247a` | 结局命名后套件全绿；5 种"对端消失"构造实测 `epipeWrites` 仍为 0（不可达由计数器持续看守） |

### 一个沿路的自我纠错（第三个例子）

原报告建议给 EPIPE 分支"加一行 `disarmWrite`"。照做之前先读 `disarmWrite`，发现两件事使其不可行：

1. 它开头就有 `if (!c->wbuf.empty()) return;`——而 EPIPE 恰恰只在 `wbuf` 非空时到达，所以这行防御是**空操作**；
2. 若为了让它能生效而放宽这个守卫，它会把事件掩码 MOD 成 `EPOLLIN`，在 P1-1b 修过的"peer FIN 停靠排空"场景里把**读自旋**又放回来——用一个新自旋去换一个旧自旋。

顺着往下查才看清真正的病灶：两个结局共用返回值 0。这也解释了为什么没人动过它——按接口的字面读法去改，一定改错。第一版修法就是把 0 当作了"写失败"的终态，结果把**每一次健康排空都变成了一次中止**，四条 close_semantics 断言同时转红（包括第二轮修的两条：'every queued reply survived the peer's half-close'、'no round loses an answer it had already produced'）。

沉淀：**一个返回值承担两种语义时，调用方的任何加固都是在赌运气。** 先把结局分开命名（`DrainOutcome`），再谈处理。

### 关于"不可达"的表述边界

`epipeWrites` 五种构造全为 0，只能说明**没触发成功**，不能证明不可达。加这个计数器的用意正是让这个判断保持可测量：一旦哪天它动了，说明上面的守卫推理失效，需要回头复查 `DrainOutcome::Dead` 的处置是否还够。同理 `eofReads=1` 说明五种场景里对端 FIN 都被正常识别且没有出现读自旋。

### 验证矩阵（最终回归，全部通过）

```
cpplint      控制文件 2 findings（证明它能报错）；tcp-server-mt.h 0 findings
exit-code 哨兵 9 项全 [OK]（false→1、return 7→7、timeout→124 …）
plain        close_semantics(默认 / CS_REACTORS=1)、backpressure、fault_injection   rc=0
asan+ubsan   同上 4 项   rc=0
tsan         同上 4 项   rc=0
Result: all configurations passed
```

### 仍然开放（有意未改）

1. **上一轮遗留两项**：用户回调抛异常杀死 reactor（文档化契约，静默吞掉会掩盖应用层 bug）；`wait()` 200ms 轮询（已知设计取舍）。
2. 待核实 #2、#3 已于本日关闭，见上表与第六节的单独说明。
