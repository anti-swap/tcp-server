# 第六轮审查：tcp-server-mt.h（HEAD `93e1673`）

审查时间：2026-10-18
范围：`tcp-server-mt.h`（2436 行）+ `verify/` 套件覆盖面
方法：静态通读 + 对每条怀疑单独写探针实测（与前五轮同一条规矩：没有运行时证据的不写成缺陷）

前五轮已关闭的主题本轮不再重复：关闭语义与 FIN 顺序、doTick 迭代期关闭、drain 的双语义零值、
停靠 MOD 的成功/失败两路、读预算下的 backlog、`framesSent` 计量点、idle 时钟按字节计时、
teardown 的分配保护、PING 覆盖率、wbuf 容量分支、冗余解析、requestStop 的取舍、shim 与主机侧编译检查。

## 摘要

**状态：7 条全部处置完毕**（5 次提交 + 报告）。下面的表格已更新为处置结果，
各条的"处置"小节写在原分析之后，原分析一字未改——结论变了的地方在处置里说明，
包括一处**本轮分析写错、被实测推翻**的（R6-4 的幽灵连接去向）。

| 级别 | 条目 | 性质 | 证据 | 处置 |
|---|---|---|---|---|
| P1 | R6-1 在关闭回调里丢弃最后一个 owner → 进程 SIGABRT；头文件明确说这样用是允许的 | 契约与实际相反 | `probe_destroy_in_callback.cpp` | 契约改回实际 + 析构处诊断 `b7af333` |
| P2 | R6-2 `Config::port` 默认 0 = 内核选端口，但没有任何访问器能读回它 | 可用性：start() 成功但服务不可达 | `probe_api_contract.cpp` | 加 `port()` `8d1ed79` |
| P2 | R6-3 `sendTo()` 对已进入关闭流程的连接返回 `Ok`，而 reactor 必然拒收 | 准入检查比 reactor 弱一级 | `probe_api_contract.cpp` | 准入对齐 + `closing` 改原子 `5e6ff1a` |
| P3 | R6-4 `submitNewConnection` 的回滚路径只 `unregisterConn`，连接残留 | 幽灵连接 | `probe_rejected_conn.cpp`（本轮实测） | 单一拆除出口 `35a4ad5` |
| P3 | R6-5 `parseFrames` 的 "scratch too small" 分支不可达 | 死代码（与 R5-4 同类） | 静态 | 保留分支 + 写不变量 `35a4ad5` |
| P2 | R6-6 测试盲区：`SendResult` 四个失败态、`acceptErrors`、`hup/err` 分支零覆盖 | 测试盲区 | 新增 `verify/send_results.cpp` | 已固化 `1bdb7ac` |
| P3 | R6-7 `submitNewConnection` 外层 catch 记 `queueDrops`，accept 循环同时记 `rejected` | 记账不一致 | 静态 | 一处失败一个计数器 `35a4ad5` |

---

## R6-1（P1）在关闭回调里丢弃最后一个 owner，进程必定 abort

头文件第 539-543 行对回调的约定是：

> Handlers run concurrently across workers and must not throw or destroy this server.
> ... stop from a callback requests shutdown only; the controlling thread must
> subsequently call stop to join threads. Close/state-false callbacks are deferred:
> they run on the worker thread after the closing connection's stack frame has
> unwound, **so they may drop the last external owner of this server**.

最后一句是被允许的用法。做一遍：

```cpp
server->setCloseHandler([&](uint64_t, const std::string&, const char*) {
    g_server.reset();          // 最后一个 owner 在这里消失
});
```

`probe_destroy_in_callback.cpp`（场景跑在 fork 的子进程里，父进程报告退出状态）：

```
== A. stop() from the close callback (the documented flow) ==
child: stop() returned from the callback
parent: child exited 0

== B. dropping the last owner from the close callback ==
child: close callback (close linger expired); dropping the last owner
terminate called without an active exception
parent: child died from signal 6 (SIGABRT)
```

链路是三行代码互相矛盾：

1. `~MultiReactorServer()` → `stop()`；
2. `stop()` 开头 `if (callbackServer_ == this) { requestStop(); return; }`（`:2126`）——
   回调线程不能 join 自己所在的线程，所以这里只请求停止就返回，**不 join、不清理**；
3. 析构函数体返回，成员逆序析构：`acceptThread_`（`:704`）先于 `reactors_`（`:703`），
   它仍是 joinable → `std::thread::~thread` → `std::terminate()`。
   `terminate called without an active exception` 正是这个析构函数的措辞。

A 场景说明这不是"回调里不能碰 server"：`stop()` 从回调调用是完全正常的。坏掉的只有
"丢弃最后一个 owner"这一支，而它恰好是头文件主动承诺支持的那一支。

**同一个类里有两条互相否定的约定**：上一句说"控制线程必须随后调用 stop() 来 join 线程"，
下一句说回调"可以丢弃最后一个 owner"。后者一旦发生，控制线程就不存在了。

**修法（建议，两件事都做）**

1. 删掉"may drop the last external owner"那句，改成实际成立的不变式：回调可以调 `stop()`，
   但最后一个 owner 只能在 `stop()` 已经 join 完线程之后释放。这句注释现在是邀请人踩坑。
2. 在 `~MultiReactorServer` 里显式检测 `callbackServer_ == this`，向 stderr 写出
   "destroyed from its own callback; threads cannot be joined here"再让它终止。
   现在的 `terminate called without an active exception` 既不提 server 也不提原因，
   而这个文件一贯的立场是"读不出来等于没发生"。

### 处置（已修，`b7af333`）

两件都做了：

1. 契约那句改成实际成立的不变式——延迟执行让回调离开了**连接的栈**，不是离开了
   **server 自己的线程**；最后一个 owner 只能在控制线程 `stop()` join 完线程之后释放。
2. `~MultiReactorServer` 检测 `callbackServer_ == this`，向 stderr 写出
   `[fatal] MultiReactorServer destroyed from inside its own callback: ...` 再让 terminate
   发生。

探针同时改成"断言"：把每个子进程的 stderr 重定向到文件（会 abort 的子进程没有机会自己报告），
父进程读回并检查那句话在不在。修前：

```
== B. dropping the last owner from the close callback ==
child: close callback (close linger expired); dropping the last owner
terminate called without an active exception
parent: child died from signal 6 (SIGABRT)
```

修后：

```
parent: child died from signal 6 (SIGABRT)
child stderr: [fatal] MultiReactorServer destroyed from inside its own callback:
  stop() cannot join the calling thread, so the accept/worker threads are still
  joinable and std::thread::~thread is about to terminate. ...
parent: the abort is named on stderr: yes
```

A 场景仍然 exit 0，且报告 `named on stderr: no`——诊断只在契约真的被违反时出现。

**被否决的方案**：让 worker / accept 线程持有一份共享 core state 的 `shared_ptr`，
使 shell 被销毁后线程仍能安全跑完（真正修好这个用法）。方向正确，但要改的是整个文件的
`server_` 访问方式，代价与收益不成比例；而上面第 1 条把契约改回真实情况之后，
这个用法本来就不该被支持。已记在此处，避免下一个人重新提议一遍。

---

## R6-2（P2）`Config::port` 默认是 0，而没有任何东西能告诉你它监听在哪

`Config::port` 的默认值是 0（`:550`），传给 `getaddrinfo` 就是"内核请随便挑一个"。
`start()` 接受它，`initListenSocket()` 成功 bind，一切返回 true。

问题在于：**没有任何访问器能读回实际端口**。`cfg_.port` 不被回写，类上也没有
`port()` / `boundPort()`。整个公开接口里只剩 `connectionCount()`、`connectionIds()`、
`stats()`——都不含端口。`probe_api_contract.cpp`：

```
A. Config::port = 0 (the default)
   start() = true, connectionCount() = 0, cfg.port still 0
   reachable: connectionCount 0, connectionIds 0, stats -- no port
   with port=11418: start() = true, client connect = ok
```

也就是说：**用默认 Config 构造的 server 能启动、报告成功、然后没有任何客户端能连上它**，
而且这件事在统计里不留痕迹（`accepted` 永远是 0，因为从来没人能连进来）。
对比 `backlog <= 0`——那是个同等"能被预测到的启动期错误"，`start()` 是拒绝它的。

**修法**：bind 成功后 `getsockname()` 取实际端口，存进一个独立成员（不要回写 `cfg_.port`，
否则 stop() 之后再次 start() 会去绑那个临时端口），并暴露 `uint16_t port() const noexcept`。
若不想扩接口，退一步是在 `start()` 的校验里拒绝 `port == 0` 并说明理由——但那样就丢掉了
临时端口在测试里的用处，而 `verify/` 里的探针正是靠自己先 `pickPort()` 绕开这件事的。

**这不是测试问题**：套件里每个用例都自己挑端口，所以这个洞一直在，只是没人从它上面走过。

### 处置（已修，`8d1ed79`）

加了 `uint16_t port() const noexcept`，值来自 bind 之后对监听套接字 `getsockname()`，
存在独立成员 `boundPort_` 里：

- **不回写 `cfg_.port`**：否则 `stop()` 之后再 `start()` 会去绑上一轮的临时端口，
  把一个看得见的洞换成一次看不见的错绑。
- **没有套接字就没有端口**：`start()` 之前与 `stop()` 之后都是 0。为此把"关闭监听套接字"
  收成一个函数 `closeListenSocket()`——五个调用点各自关 fd、各自忘了把已发布的端口收回去，
  正是这两件事走散的方式。
- IPv4 / IPv6 两路都读（`sockaddr_in` 与 `sockaddr_in6`），shim 因此补了 `sockaddr_in6`。

`probe_api_contract.cpp` 的 A 场景从"描述缺失"变成断言：它连的是 `port()` 报出来的号，
而不是自己挑的号。

```
[PASS] port() reports the port the kernel chose          (kernel chose 13766)
[PASS] a client can connect to port() without being told the number
[PASS] and the server counts that connection
[PASS] port() is 0 again after stop(): no socket, no port
[PASS] port() resolves an IPv6 listener too
[PASS] port() echoes an explicit Config::port
[PASS] cfg.port is left alone: no write-back
```

---

## R6-3（P2）`sendTo()` 对已进入关闭流程的连接返回 `Ok`

`sendTo()` 的准入检查（`:2365`）：

```cpp
if (c->isClosed.load(std::memory_order_acquire)) return SendResult::Closed;
```

而 reactor 里真正执行的检查（`submitSend`，`:935`）：

```cpp
if (c->isClosed.load(std::memory_order_acquire) || c->closing) { /* 拒收 */ }
```

`closing` 一旦置位，`submitSend` 就**必然**拒收该帧。所以 `sendTo()` 在 `closing`
已置位时返回 `Ok`，是一张不会兑现的承诺。实测（`probe_api_contract.cpp`）：

```
closeConnection() -> true, still in the connection table: yes
sendTo() to a connection whose close is in progress: Ok
  and what that Ok bought: bytesSent +30, queueDrops +1, bytesDropped +12, closed +0
```

那 12 字节进了 `bytesDropped`，调用方拿到的是 `Ok`。头文件里"Ok means queued, not
delivered. Shutdown or disconnection can discard work"替它开脱了一半——但这不是 shutdown
也不是 disconnection，是**应用自己刚刚要求的关闭**，一个完全可以在调用点报出来的状态。

反过来说，加上 `closing` 检查不会伤到任何合法路径：`drainInputThenClose()` 里的
`handleRead()` 跑在 `parkClose()` 之前，那一轮 handler 产生的应答发生时 `closing` 仍是
false（这是 N1 / R5-1 两轮修好的东西，不会被推翻）。

**修法**：`sendTo()` 的准入检查改成与 reactor 一致——`isClosed || closing` → `Closed`。
两处判断同形之后，"Ok"就重新等于"至少进了队列"。

### 处置（已修，`5e6ff1a`）

`sendTo()` 现在检查 `isClosed || closing`。为此 `Conn::closing` 从 `bool` 变成
`std::atomic<bool>`（acquire 读 / release 写，与紧邻的 `isClosed` 同形）：它一直由
**owner reactor 线程**写、也一直只被那个线程读，直到这一条——`sendTo()` 跑在应用调用的
任意线程上，读一个非原子 bool 是实打实的竞态，TSan 会报。

实测（基线取在关闭生效之后，所以被关闭本身拒掉的积压不会被算到这次发送头上）：

```
[PASS] the closing connection is still in the table
[PASS] sendTo() to a connection whose close is in progress: Closed, not Ok
[PASS] and what it answered bought no drops either
  bytesSent +0, queueDrops +0, bytesDropped +0, closed +0
```

---

## R6-4（P3）两条回滚路径留下幽灵连接

`submitNewConnection()`（`:886-901`）有两条失败出口，都只调 `unregisterConn(c.get())`：

```cpp
try {
    fdIndex_[c->fd] = c.get();
    conns_[id] = c;                       // 可能抛 bad_alloc
} catch (...) {
    unregisterConn(c.get());
    server_->statsRef().rejected.fetch_add(1, ...);
    return;
}
if (!server_->onConnectionOpened(id, c)) {   // 可能返回 false
    unregisterConn(c.get());
    server_->statsRef().rejected.fetch_add(1, ...);
    return;
}
```

`unregisterConn()` 做的是：erase `fdIndex_`、EPOLL_CTL_DEL、`close(fd)`、`fd = -1`。
它**不动 `conns_[id]`，也不置 `isClosed`**。于是 reactor 的 `conns_` 里留下一条
`fd == -1 && !isClosed && !closing` 的连接：

- `doTick()` 会遍历到它，按 `lastRecvMs` 判成 idle → `closeConn(graceful)` → `parkClose()`
  → `tryBeginWriteShutdown()` 因 `fd < 0` 返回 `c->finSent`（false）→ park 后什么也不会发生；
- 真正清掉它要等 `closeLingerMs` 到期走 `expired` 那条路（`:1831`），即
  **idleTimeoutMs + closeLingerMs** 两级计时；
- 这段时间里它占着 `conns_` 条目和一个 admission 槽位；
- `cleanup()` 能清掉它（`teardownClosed` 的 CAS 会成功），所以不会死循环，也不会泄漏到进程结束。

触发条件是 bad_alloc（两条都是分配失败），罕见但真实。

**修法**：这两条出口改为走 `teardownClosed(c.get(), "registration failed", false)`，
或者至少 `conns_.erase(id)` + `c->isClosed.store(true)`。回滚路径与正常拆除路径共用
同一个出口，是这个文件在别处已经用过两次的做法（"Single teardown path"，`:1451`）。

### 处置（已修，`35a4ad5`）——并更正上面"幽灵连接去向"的推断

两条出口现在都走 `discardRejectedConn()`：`unregisterConn()` + `conns_.erase(id)` +
`onConnectionClosed(id)` + `isClosed` 置位 + 立刻归还 admission 名额。**没有**走
`teardownClosed()`：它会记 `closed`（且非 graceful 时记 `aborted`），而这条连接从未被
`accepted`，记进去就让 `accepted` 与 `closed` 永久对不上。

**上面那段分析里"真正清掉它要等 closeLingerMs 到期走 expired 那条路（idleTimeoutMs +
closeLingerMs）"是错的**，实测推翻：

- idle 分支会 park 一个 `tryBeginWriteShutdown()` 起不来的关闭（fd < 0），于是只留下
  `closing` 置位；
- expired 分支的判定里有 `c->fd >= 0`，fd == -1 **永远不满足**。

所以幽灵不会被任何一次 tick 收走，它一直活到 reactor 关停时的 `cleanup()`，在那里以
"server shutdown" 被 `closeConn()`——记 `closed`、记 `aborted`——**一条从未被接受的连接**。
本轮的"待核实"项（未注入实测）因此关闭：

```
BEFORE   while running -- accepted 0, rejected 1, closed 0, aborted 0
         after stop()  -- accepted 0, rejected 1, closed 1, aborted 1
         [FAIL] a connection that was never accepted is never closed
AFTER    after stop()  -- accepted 0, rejected 1, closed 0, aborted 0
         [PASS]
```

注入落在"注册成功之后"的那一步才有幽灵（见 `probe_rejected_conn.cpp` 的表：N=1/N=2
是插入自身失败，插入失败就不会有条目；N=3 才是 `onConnectionOpened` 的 map 插入失败）。
**推论**：`onConnectionOpened()` 返回 false 的那条路才是唯一能造出幽灵的路。

---

## R6-5（P3）"scratch too small" 不可达

`parseFrames()`（`:1438-1441`）：

```cpp
if (len > 0) {
    if (len > frameScratch_.size()) {
        closeConn(c, "scratch too small", false); return;
    }
```

`frameScratch_` 在 `SubReactor::start()` 里 `resize(MAX_FRAME_PAYLOAD)`（分配失败则
start 直接失败），而 `len` 在 10 行之前已被 `if (len > MAX_FRAME_PAYLOAD) closeConn("frame too large")`
夹住。所以 `len <= MAX_FRAME_PAYLOAD == frameScratch_.size()` 恒成立，这个分支不可达。

与 R5-4 是同一类：分支本身无害（真到了也会正确断开），但它读起来像一条会被走到的防线。
**修法**：按 R5-4 的处理方式——保留分支（替代行为是越界写），把上面的不变式写在分支上方。

### 处置（已处置，`35a4ad5`）

分支保留，不变式写在它上方（`frameScratch_` 在 `start()` 里 resize 到 `MAX_FRAME_PAYLOAD`，
分配失败则 start 直接失败；`len` 十行前已被夹到 `MAX_FRAME_PAYLOAD`）。

---

## R6-6（P2）测试盲区：`sendTo` 的四个失败态、`acceptErrors`、`hup/err`

`verify/` 里没有任何断言碰过下面任何一个：

| 对象 | 覆盖 |
|---|---|
| `SendResult::NotConnected` / `TooLarge` / `Overloaded` / `InvalidArgument` | 零 |
| `Stats::acceptErrors`（EMFILE/ENFILE 路径） | 零 |
| `loop()` 里的 `EPOLLHUP \| EPOLLERR` → `closeConn(c, "hup/err", false)` | 零 |

`sendTo()` 的四个失败态是应用从一个 send 上能得到的全部信息（除 `Ok` 外），
所以它"正确但无人守护"是最糟的一种状态：任何一次编辑都能改掉它而全部配置仍然全绿。

本轮首跑，行为都是对的（`probe_api_contract.cpp`）：

```
to a live connection, 4 bytes : Ok
to an id that was never issued: NotConnected
payload over MAX_FRAME_PAYLOAD: TooLarge
data = nullptr with len = 5   : InvalidArgument
after saturating maxPendingBytes: Overloaded
```

**建议**：把上面四行固化成 `verify/` 里的一个用例（它们现在是绿的，正是固化的时机），
`acceptErrors` 与 `hup/err` 各补一条——前者可以在子进程里把 RLIMIT_NOFILE 压低后
`start()`（注意 `start()` 会尝试抬高软限制，要压硬限制），后者让对端发完请求后 RST。

### 处置（已固化，`1bdb7ac`）

新增套件成员 `verify/send_results.cpp`，接进 `run-in-wsl.sh`：

```
[PASS] a live connection: Ok
[PASS] an id that was never issued: NotConnected
[PASS] a payload past MAX_FRAME_PAYLOAD: TooLarge
[PASS] data == nullptr with len > 0: InvalidArgument
[PASS] a frame past maxPendingBytes: Overloaded
[PASS] a connection whose close is in progress: Closed
[PASS] and it bought no drops: the refusal happens before any accounting
[PASS] refused accepts are counted, not swallowed   (child: accepted 12, acceptErrors 28)
```

两个细节是重点而不是布景：

- `Overloaded` 用"帧大于 32 字节上限"达成，不去和排空赛跑——靠淹没对端来撞上它，
  是让用例的成败取决于一次竞速。
- `acceptErrors` 在 fork 出来的子进程里压 **硬** 限制到 20，父进程连 40 个 peer 撑爆它。
  必须两个进程（上限是按进程的，耗尽的一方不能同时是不停敲门的一方），必须是硬限制
  （`start()` 会把软限制抬上去，抬得动的 ceiling 不是 ceiling）。
  stderr 上那句"[warn] RLIMIT_NOFILE=20 too low"是这个机制在自我介绍。

`hup/err` 补了，但在本机是 `[SKIP]`，并且写明理由：先用一对裸 socket 问内核"被中止的
连接在 epoll 上是否可见"。本机（WSL1，4.4.0-19041-Microsoft）把它报成干净 FIN，
`EPOLLHUP|EPOLLERR` 永远不投递——这与 `close_semantics` 里 `kernelHonoursZeroLinger()`
的结论是同一件事的两种观测。**先问后测**，否则断言的对象就从头文件变成了宿主机。

---

## R6-7（P3）一次失败记进两个语义不同的计数器

`submitNewConnection()` 的外层 catch（`:914-918`，构造 task 失败）记 `queueDrops`；
`acceptLoop()` 看到返回值 false 后记 `rejected`（`:2303`）。所以一次"新连接没能交出去"
同时进了两个计数器。而 `queueDrops` 的注释是"全帧入队被拒次数"——属于发送路径，
和新连接无关。**修法**：外层 catch 改记 `rejected`，与同一函数的另外三条失败出口一致。

### 处置（已修，`35a4ad5`）——比建议多走一步

建议是"外层 catch 改记 `rejected`"，但那样**同一个失败会在 `rejected` 里记两次**
（catch 一次，accept 循环看到 false 再一次）。所以改成：外层 catch **不记**，
由看到 false 的调用方记那唯一的一次 `rejected`。`queueDrops` 从此只剩它字面上的意思。

顺带发现同一类误记还有一处，本轮没列进表里：`onConnectionOpened()` 的 catch
（server 侧 map 插入失败）也记 `queueDrops`，那里同样没有帧。已一并改掉。

---

## 待核实清单（处置后）

- `[已否决: R6-1 的结构性修复成本]` —— 让 worker/accept 线程持有共享 core state 需要改
  所有 `server_` 访问点，代价与收益不成比例；契约改回真实情况之后，那个用法本就不该被支持。
  理由已写进 `b7af333` 的提交信息，不再重新提议。
- `[已确认无风险: R6-2 是否有人依赖 port=0 的当前行为]` —— 采的是"加访问器"而不是
  "在 `start()` 里拒绝 0"，`start()` 的接受范围一字未动，临时端口在测试里的用处保留。
- `[已实测: R6-4 在真实 bad_alloc 下的表现]` —— `probe_rejected_conn.cpp` 注入了。
  结论同时**推翻了本轮分析里"幽灵会在 idle+linger 后被回收"的推断**（见 R6-4 处置）：
  它活到 reactor 关停时的 `cleanup()`。
- `[已复核: R6-3 是否存在"closing 已置位但仍应收下"的路径]` —— 修复后跑了全套
  （含 `ping_pong` 与 `close_semantics`，两者正是守着"关闭前那一轮 handler 的应答仍要发出"
  这件事的用例），无回归。
- `[仍然开放: hup/err 的真实覆盖]` —— 用例写好了，但本机内核不投递 `EPOLLHUP|EPOLLERR`，
  所以它在这里恒为 `[SKIP]`。换到真 Linux 内核上会自动生效；在那里跑一次是本轮唯一
  没能自己关闭的核实项。

## 建议顺序（已按此执行）

1. **R6-1**（唯一一条会让进程没的）：先改契约注释，再加析构处的诊断。`b7af333`
2. **R6-2**：加 `port()` 访问器（同时把 `probe_api_contract.cpp` 的 A 场景变成断言）。`8d1ed79`
3. **R6-3**：`sendTo()` 准入检查与 `submitSend` 对齐。`5e6ff1a`
4. **R6-6**：把四个 `SendResult` 固化成套件成员。`1bdb7ac`
5. **R6-4 / R6-5 / R6-7**：三条清理。`35a4ad5`

## 本轮新增证据

- `verify/evidence/probe_destroy_in_callback.cpp` —— R6-1，A/B 两个场景（回调里
  `stop()` vs 回调里丢弃最后一个 owner），跑在 fork 的子进程里以观测进程是否存活；
  每个子进程的 stderr 重定向到文件，由父进程读回（会 abort 的子进程自己没有机会报告）
- `verify/evidence/probe_api_contract.cpp` —— R6-2 与 R6-3（默认端口、`sendTo` 五态、
  关闭中发送的字节去向）；A/B 两个场景在修复后从"打印观察值"改成 `check()` 断言
- `verify/evidence/probe_rejected_conn.cpp` —— R6-4，注入分配失败到注册路径的指定一步
  （`--wrap=epoll_ctl` 布防 + `--wrap=_Znwm` 抛一次），并测量"从未被接受的连接会不会
  被记成 closed"
- `verify/send_results.cpp` —— R6-6，套件成员（不是探针）：五个 `SendResult` 各一条断言 +
  `acceptErrors`（子进程压硬 RLIMIT_NOFILE）+ `hup/err`（先问内核，本机 SKIP）

## 回归状态

HEAD `35a4ad5`。见下方"回归"一节。

## 回归

- Linux 侧 `verify/run-in-wsl.sh`：plain / asan+ubsan / tsan × 7 组 = **21 配置全 rc=0**
  （本轮新增 `send_results` 一组）。TSan 这一轮格外重要：`Conn::closing` 从 `bool` 改成
  `std::atomic<bool>`，因为 `sendTo()` 开始从应用线程读它。
- cpplint：`tcp-server-mt.h` 0 findings（`run-in-wsl.sh` 每次先 lint 一个必定报错的对照文件）。
- 主机侧 `verify/build.sh`：18 TU 全绿无警告（含 3 个新探针 + 1 个新套件成员）。
- 本轮改了 `tcp-server-mt.h`，改动集中在：回调契约注释与析构诊断、`boundPort_` 与
  `port()`、`closing` 原子化与 `sendTo` 准入、`discardRejectedConn()` 单一回滚出口、
  `parseFrames` 不变量注释、两处 `queueDrops` 记账。
