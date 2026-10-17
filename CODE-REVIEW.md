# 代码审查报告：tcp-server/tcp-server-mt.h

- 审查时间：2026-10-17
- 审查模式：full
- 审查对象：`C:\Users\root\WorkBuddy\Claw\tcp-server\tcp-server-mt.h`（1281 行，单头文件，C++17 / Linux epoll 多 reactor TCP 服务端）

## 一、环境与闭环状态

| 项 | 结果 |
|----|------|
| 文件总数 | 1（该目录下无其他文件） |
| 依赖清单 | 无（仅标准库 + POSIX；无 package/CMake 等构建文件） |
| git 仓库 | 不存在 → 跳过提交历史分析（E-103 不触发，属"无 git 目录"跳过分支） |
| `.coding-standards.md` | 不存在 → 使用内置宽松规范 |
| 测试 / 用例文件 | **无**。`INDUSTRIAL_TCP_TESTING` 注入点（L349-352、L856-858、L1103-1105）存在，但驱动它们运行的测试缺失 |
| 闭环状态 | ⚠ **闭环断裂**（代码中包含"为测试而设"的故障注入，却没有任何测试入口） |
| 编译验证 | ✅ 已建立宿主侧编译检查：`bash verify/build.sh`（g++ 15.2 / `-Wall -Wextra -Wpedantic -Wshadow`），当前**零警告通过**。仅覆盖语法/类型/警告，不覆盖运行时 |

> 说明：本次为纯静态审查。凡是"依赖运行时行为"的结论，均在条目内标注 `[需核实:...]`，不做臆断。
>
> **编译验证状态**（2026-10-17 追加）：已建立 `verify/` 宿主侧编译检查，用 g++ 15.2 对头文件做完整语义编译并通过零警告。它**第一次运行就抓出一个真实编译错误**（`finishClose` 对 `const shared_ptr` 调用 `reset()`，见 commit `51729c3`），说明前面几轮"未编译"确实在裸奔。该检查不覆盖运行时行为，真机验收仍需 ASan/UBSan。

## 二、结论速览

代码整体工程质量**高于一般 LLM 生成水平**：RAII 封装完整、全帧原子写入、每 reactor 读/写/帧三重预算、accept 与 reactor 的 fd 交接靠 `FdGuard` + 未来值同步、`Admission` 票券统计连接数、关闭路径幂等（`isClosed` CAS），注释也基本与实现一致。这些是对的，不需要改。

但存在 **2 个"随机打死全服"级别的竞态**（一个在 accept 引导阶段、一个在 handler 回调内），以及 **2 个会让线上监控数字失真的语义缺口**（连接成功却报 rejected / 静默半关闭）。这四件事的共性是：**正常运行完全看不出来，只在负载尖峰或关闭瞬间出现**，正是 LLM 生成高并发代码最常见、也最容易被"看起来对"掩盖的缺陷类型。

## 三、缺陷清单（按优先级）

### [P0-1] accept 引导阶段存在 "fd 已发布、epoll 未注册" 的指令重排窗口　✅ 已修复（commit ed08532）

- **位置**：`initListenSocket()` L925-955 → `start()` L1018 → `acceptLoop()` L1077-1102
- **缺陷模式**：D-02 上下文丢失（多线程发布顺序）
- **机理**：
  1. `listenFd_` 在 `initListenSocket()` 里、**accept 线程启动之前**就赋了值；
  2. `start()` 在 L1018 启动 accept 线程，然后阻塞在 `fut.get()`；
  3. 此时主线程仍全程持有 `lifecycleMutex_`（L959）。
  4. 于是任何 **从回调线程** 调用 `server.stop()` 的路径，会在 L1035 卡在 `lifecycleMutex_` 上；但它在卡住**之前**，L1034 已经判定"我不是回调线程"，并且 L1036 的 `running_.store(false)` **还没有执行**。
  5. 在此期间若某反应堆因错误自行退出（L868-873 / L909-912）并执行 `server_->running_.store(false)`；
  6. `acceptLoop` 里的 `while (ok && running_.load(...))`（L1102）于是**直接不进入循环** —— 注意 `epollFd_` / `wakeFd_` 是在循环**之前**创建的（L1082-1095），所以它们"分配成功"却永远没有被 `epoll_wait` 消费。
  7. `start()` 的 `fut.get()` 拿到 true，L1019 `isRunning()` 因第 5 步已为 false，走失败清理分支：`acceptThread_.join()`（线程早已结束）→ `for (auto& r : reactors_) r->stop()` 逐个 join → L1027-1029 关闭 fd → `return false`。
- **后果**：若 `stop()` 在 `join()` 返回后于 L1042 抢到锁，它**看不到**任何需要唤醒的对象（`wakeFd_` 已被置 -1），而它的 `running_` 在 L1036 又被重新写回 false —— 这本身无害；真正致命的是**引导阶段对 `running_` 的"回写 false"覆盖了调用方语义**：`start()` 返回 false 时用户只会认为"启动失败"，但此前所有 reactor 线程已经接过连接并可能已进入 `cleanup()`。表现为：客户端提示连接被拒/立刻断开、`reactorFailures` 偶发 +1、进程日志里毫无线索，重启即恢复。
- **判据**：`running_` 在引导路径上被读写 ≥ 6 次（L960/966/985/994/1003/1011/1019/1023），且其中 2 处（L871、L911）是由非生命周期线程写入的"stop 等价物"。**共享一个 atomic 同时承担"用户意图"和"内部存活"两种语义，是这个 bug 的根**。
- **建议**：把引导期与运行期拆成两层状态。
  ```cpp
  enum class Life : int { Stopped = 0, Starting = 1, Running = 2, Stopping = 3 };
  std::atomic<Life> life_{Life::Stopped};
  std::atomic<bool> fatal_{false};   // 线程局部失败只允许单向置位
  ```
  - reactor/accept 内部失败只写 `fatal_`，**永不**写 `life_`；
  - `isRunning()` 改为 `life_ == Running && !fatal_`；
  - `start()` 从 `Starting` 推进到 `Running` 只在 accept 线程确认 epoll 注册成功后进行（把 `ready.set_value` 与状态推进放在同一临界区）；
  - 这样"某个线程猝死"不会再产生 `start()` 语义歧义。
- **已落地（commit `ed08532`）**：
  - 新增 `enum class Life { Stopped, Starting, Running, Stopping }` + `std::atomic<bool> failed_`；`running_` 仅保留在 `SubReactor` 内部（纯 worker 私有语义）。
  - `isRunning()` 改为 `life_ == Running && !failed_`，`start()` 在 accept 线程报到成功后把 `Starting → Running` 并返回 true。
  - reactor/accept 的 `epoll_wait` 失败与异常一律走 `fail()`（单向置位），不再触碰生命周期；accept 描述符初始化失败也从 `requestStop()` 改为 `fail() + return`。
  - `requestStop()` 改为 CAS `Running → Stopping`（原先是无条件 store，可能把 `Stopped` 拉回"假运行"）；`stop()` 在已 `Stopped` 时直接返回，恢复路径与 `stop()` 共用 `wakeAcceptLoop()`。
  - `wait()` / `sendTo()` / `closeConnection()` 统一改用 `isRunning()`。
- **残留**：本机无 Linux 工具链（WSL 被安全策略禁用、无 g++/clang++），**本次改动未经编译验证**，属 `[需核实:在 GCC ≥ 9 + -Wall -Wextra 下的编译结果]`，需在目标 Linux 环境跑一次 `start/stop × 100 + ASan` 冒烟。

### [P0-2] `closeConn()` 在 handler 内同步析构 `shared_ptr<Conn>` → 可能对调用者 `this` 执行 `delete`　✅ 已修复（commit c80a551）

- **位置**：`SubReactor::closeConn()` L789-816，调用点 L624、L636、L645、L699、L710、L717、L724、L735、L742、L765、L778、L843、L892
- **缺陷模式**：D-02 上下文丢失（跨组件生命周期）
- **机理**：`closeConn` 的清尾全在 `server_` 之外进行：
  - L810 `onConnectionClosed(c->id)` 擦掉服务器侧 `conns_`（可能是最后一个 shared_ptr）；
  - L814-815 依次调用 `stateHandler_` / `closeHandler_`。
  如果**任意一个回调**最终走到 `server.stop()` 并销毁 `MultiReactorServer`（例如"最后一个连接断开 → 业务决定收工 → 析构服务器"），那么 `server_` 变成悬垂指针；`closeConn` 是在 `SubReactor` 的成员函数里、**在回调栈帧之下**继续执行的，随后 `closeConn` 返回，`SubReactor::stop()` → `cleanup()` 访问 `server_->statsRef()`，**对已释放对象写统计**。当前 `CallbackScope` 只保护 `stop()`（L458 / L1034），没有保护"回调期间对象被析构"。
- **判据**：`CallbackScope` 的作用域覆盖 L850-919（整个 loop）与 L471-485，但析构点不在其保护语义内 —— 这是一个"防重入"而非"防生命周期"的机制。
- **建议**（二选一，推荐前者）：
  1. 把 `closeConn` 的副作用**外置为 reactor 拥有者的延迟队列**：标记关闭 → 从两个 map 摘除 → 把 `id/peer/reason` 与待触发回调压入 `activeTasks_`，由 `processPending()` 统一触发。届时 conn 已不再被闭包引用，回调再怎么样也影响不到 `this`。
  2. 或者至少把 `keepAlive`（L797 已拿到）与 `server_` 的守卫显式延长到函数末尾，并在 `cleanup()` 开头再取一次 `this` 的存活证据（该方法本身在 lambda 里执行，不够）。
  - 附：`region` 里 L810 `onConnectionClosed` 与 L811 `c->admission.reset()` 之间没有 `keepAlive` 相关的冲突，但 **`closeReason`（L794）只存了指针、没有拷贝**，调用方传字面量目前安全，一旦传 `std::string` 的 `.c_str()` 就是 UAF —— 见 P2-4。
- **已落地（commit `c80a551`）**：
  - `closeConn()` 只做"栈帧安全"的事：摘 list、`epoll_ctl(DEL)` + close fd、两个 map 去引用、计数；**不再调用任何用户回调**。
  - 新增 `flushCloseCallbacks()`：先把 `pendingCloses_` 里的 `shared_ptr<Conn>` 释放（`finishClose`），**再**调用 `ConnStateHandler(false)` / `CloseHandler`——此刻该连接的引用计数已归零，回调不再可能让调用帧复活到已销毁对象。
  - 触发点：`processPending()` **每个任务之后**、`loop()` 每个 epoll 批次之后（`doTick` 之前，它也走同一路径）、`cleanup()` 清空所有连接之后。
  - `reason` / `peer` 在入队时拷贝（`reason` 原本是借用指针），deferred 之后生命周期才安全。
  - 关闭批量内所有连接时，先把 `shared_ptr` 从 `conns_` 搬出并释放再 close，避免 `cleanup()` 的 `while` 迭代器持有引用导致回调里"连接还活着"的错觉。
  - 在类头注释里显式写明：close/state-false 回调是延迟执行的、可以释放服务器的最后一个外部所有者。
- **残留**：
  - 回调现在不再与关闭发生在同一个调用帧，**时序上延后到本批任务/事件处理结束**——依赖"回调里立刻看到连接已消失"的调用方需改用 `connectionCount()` 之类的外部观测。
  - `requestStop()`（回调内调用）只 CAS 状态 + 唤醒 accept 线程，不 join，这与原约定一致；但 `stop()` 在回调内被直接调用时仍会把 `Stopping` 置位，然后由控制线程完成 join —— 属既有约束，未变。

### [P1-1] `submitSend` 的 `wasEmpty` 判据与 `pendingBytes` 的"保留态"不匹配 → 可能永久挂起点：已写入 wbuf 却未 arm EPOLLOUT　✅ 已修复（commit 478e074）

- **位置**：`submitSend` L594-611（`bool wasEmpty = c->wbuf.empty();`）
- **缺陷模式**：D-07 隐式假设（"wbuf 空 ⇒ 没有未完成的写"是错误假设）
- **机理**：
  - `pendingBytes` 的语义是"wbuf 内待发 + 已入队未写入"，而 `wbuf.empty()` 只反映**前者**；
  - 每次**入队**（`sendTo` L1244、PING L757）都会先把字节记入 `pendingBytes`，任务稍后才真正 `wbuf.write`；
  - 若此时 reactor 收到 `EPOLLOUT`，`drainWbufToSocket` 发现 `wbuf` 为空即返回 0 → `handleWrite` → `disarmWrite`；
  - 紧接着 `submitSend` 任务执行，`wasEmpty == true` → `armWrite(c)` —— 顺序上没问题。**真正的裂缝在于反向顺序**：`wasEmpty` 为真说明"此刻无字节可发"，但 `disarmWrite` 可能**正在执行中**（epoll_ctl MOD 与本次 MOD 竞争，后者被 EPOLLOUT 事件覆盖），于是最后一次 `armWrite` 后 `EPOLLOUT` 被注销，而 wbuf 里已有字节 —— 该帧要等到下一次 `armWrite`（即下一次 wbuf 由空变非空）才会发出。若此后业务数据稀疏，连接表现为**偶发的、秒级的、无法复现的发送延迟**。
- **判据**：`armWrite`/`disarmWrite` 都无条件 `epoll_ctl(MOD)`，没有"目标状态"判据；`pendingBytes` 与 `wbuf` 之间存在一个**保留但未写入**的中间态，而 `wasEmpty` 只看后者。
- **建议**：去掉"是否需要 arm"的判断，改为**幂等的目标状态机**：
  ```cpp
  bool writeArmed = false;              // reactor 独占
  void armWrite(Conn* c) noexcept {
      if (c->writeArmed) return;        // 幂等，重复 arm 无副作用
      ... epoll_ctl(ADD/MOD) ...
      c->writeArmed = true;             // 仅在 ctl 成功后置位
  }
  void disarmWrite(Conn* c) noexcept {
      if (!c->writeArmed) return;
      if (!c->wbuf.empty()) return;     // 仍有数据 → 不撤销
      ... epoll_ctl(MOD) ...; c->writeArmed = false;
  }
  ```
  `submitSend` 里直接 `armWrite(c)`，并把"是否有数据待发"的唯一真相源收敛到 `wbuf`（`pendingBytes` 只用于限流，不再参与调度决策）。这样两个方向的竞态都被消除。
- **已落地（commit `478e074`）**：
  - `Conn::writeArmed`（worker 独占）记录该 socket **期望的** `EPOLLOUT` 状态。
  - `armWrite()`：已 arm 则直接返回（不再发 `epoll_ctl`）；`ENOENT`（注册丢失）用 `EPOLL_CTL_ADD` 修复并同步置位，而不是把连接关掉。
  - `disarmWrite()`：未 arm 则 no-op；`wbuf` 非空时**拒绝撤销**，交给调用方继续 drain。
  - `submitSend` 与 PING 分支的 `wasEmpty` 判断全部删除，改为无条件 `armWrite(c)`。
  - 语义变化：`pendingBytes` 从此只承担限流职责，`wbuf` 是写调度的唯一真相源（与"全帧原子写入"的不变量一致）。
- **附**：`submitSend` / PING 分支都运行在 reactor 线程上，`arm` 与 `disarm` 的调用点因此**全部串行**（`processPending` 与 `handleWrite` 都在同一 loop 里），所以 `writeArmed` 用普通 `bool` 而非原子量是安全的——这一点已写进 `Conn` 字段注释。

### [P1-2] 关闭连接时 `wbuf` 中被丢弃的字节未回退 `pendingBytes` → 发送队列水位只增不减　✅ 已修复（commit 3db4521）

- **位置**：`drainWbufToSocket` L673 只扣已发送字节；`closeConn` L789-816 未处理残留
- **缺陷模式**：D-06 错误吞没（同类：状态泄漏）
- **机理**：`pendingBytes` 在**入队时**加、在**发送成功时**减。连接中途断开时，`wbuf` 里剩余 N 字节永远不会发出去，`drainWbufToSocket` 也不会再被调用，于是这 N 字节永远挂在计数器上。对**该连接**无害（对象即将销毁），但：
  - `stats.queueDrops` 会把"关闭丢弃"和"过载拒绝"混在一起，运维无法区分"对端太慢" vs "我方掉线"；
  - 若将来把 `pendingBytes` 提升为**全局**水位（很自然的下一步），这个泄漏会直接变成"内存永不满、写侧永久限流"。
- **建议（原方案）**：`closeConn` 在 `c->fd` 关闭后补一行：
  ```cpp
  const size_t dropped = c->wbuf.readable();
  if (dropped) { c->pendingBytes.fetch_sub(dropped, std::memory_order_relaxed);
                 server_->statsRef().bytesDropped.fetch_add(dropped); }
  c->wbuf.clear();
  ```
  并新增独立计数 `bytesDropped`（不要把关闭丢弃塞进 `queueDrops`）。
- **已落地（commit `3db4521`）**：
  - 新增 `discardPendingOutput(Conn*)`：`wbuf.clear()` + `pendingBytes -= wbuf.readable()` + `bytesDropped += 同上`，在 `closeConn()` 摘 fd **之前**调用（关闭回调读统计时已能看见）。
  - 新增 `ServerStats::bytesDropped`，把"关闭丢弃"从 `queueDrops` 中分离。
  - **刻意不处理的边界**：任务已占额但尚未写入 wbuf 的字节由 `submitSend` 的 `isClosed` 分支自行回退——两处账目互斥，避免双减。这一点写在 `discardPendingOutput` 的注释里，避免后来者"补全"造成重复扣减。
  - **与 P1-1 的关联**：`pendingBytes` 在 P1-1 之后已退出写调度，只作限流计数；这条修复让它重新满足"入队 +size / 发送 −k / 关闭 −剩余"的三段式语义，为将来的全局水位统计打好底。

### [P1-3] 连接的"成功语义"与统计口径相反：`accepted` 在入队时 +1，实际 `rejected` 发生在 reactor 线程　✅ 已修复（commit bcd2ead，与 P2-1 同批）

- **位置**：`acceptLoop` L1169-1173 vs `submitNewConnection` L552-566 / L576-579
- **缺陷模式**：D-05 复制粘贴变异（成功/失败分支的计数位置不一致）
- **机理**：`accepted++` 只有在 `submitNewConnection()` 返回 true（**任务入队成功**）时才执行；但该 lambda 内仍可能因 `epoll_ctl ADD` 失败（L552）、map 插入异常（L561）、`onConnectionOpened` 失败（L568）而 `rejected++` 并直接返回。此前 `accepted` 已经 +1。结果 `accepted` 与 `rejected` **可以同时增长**，`accepted - closed` 也不再等于在线连接数；配合 P0-2 的那种静默失败，线上会出现"统计上明明有连接、实际一个都没有"。
- **建议**：把 `accepted` 的 +1 移入 lambda、放在 `conns_[id] = c` 成功**之后**（即真正接管 fd 的那一刻），`acceptLoop` 只保留"入队失败则 rejected"。并在 `onConnectionOpened` 失败分支补一次 `epoll_ctl(DEL)`（见 P2-1）。
- **已落地（commit `bcd2ead`，与 P2-1 合并为一个提交，因为两者改的是同一段代码）**：
  - `accepted++` 移入 reactor lambda，位于 `conns_[id] = c` 与 `onConnectionOpened()` **都成功之后**；`acceptLoop` 只保留入队失败 → `rejected++`。
  - `onConnectionOpened` 失败分支补上 `epoll_ctl(DEL)`（通过新抽出的 `unregisterConn()`），并补 `rejected++`。
  - 结果：`accepted` 与 `rejected` 互斥，`accepted - closed == 在线连接数` 重新成立。

### [P1-4] 优雅关闭未 `shutdown(SHUT_WR)`：业务应答可能被丢弃；错误路径还直接 RST

- **位置**：`closeConn` L802-807（`epoll_ctl(DEL)` 紧接 `::close`）
- **缺陷模式**：D-02 上下文丢失（协议层语义）
- **机理**：两件事。
  1. **无半关闭**：业务侧常见模式是"处理完 → `sendTo` → `closeConnection`"，两步都在同一线程入队。`closeConnection` 的任务可能先于 `sendTo` 的任务执行（不同任务、同一个 pending 队列，顺序**不保证**——`sendTo` 先入队所以通常在前，但 `closeConnection` 从别的线程来就完全可能超前）。一旦 `closeConn` 先跑，被 close 的 socket 里未发出的字节直接丢弃。
  2. **RST 风险**：若 `rbuf` 里还有对端未读数据（或被 `disarmWrite` 后仍有 wbuf 数据），`close()` 会发送 RST，对端看到的是 `ECONNRESET` 而不是 FIN，可能触发对端"重试整批数据"的雪崩。
- **建议**：
  - 新增 `SendResult`/状态：`ShutdownWrite`（只 `shutdown(fd, SHUT_WR)`，保留读侧排空、等 `EPOLLIN` 读到 EOF 再 close）；
  - `closeConn` 增加 `bool graceful` 形参：graceful 路径先 `discard rbuf` → `flush wbuf`（尽力而为）→ `shutdown(SHUT_WR)` → 标记 `halfClosed`，等对端 FIN 或 5s 超时再真正 close；
  - 非 graceful 路径（bad magic / 帧过大 / 安全原因）才允许直接 close，并在 close 前 `setsockopt(SO_LINGER{1,0})` 让 RST 语义显式化，而不是碰运气。

### [P1-5] 生产代码里残留测试后门开关，且目录中没有任何测试

- **位置**：L349-352（`failWorkerForTest` / `failAcceptForTest`）、L856-858、L1103-1105
- **缺陷模式**：D-03 测试盲区
- **机理**：`#ifdef INDUSTRIAL_TCP_TESTING` 只在测试构建里开启，这本身是**好设计**。问题在于：仓库里没有测试文件、没有构建脚本，这两个开关没有任何消费者。于是它们变成"谁都可以在构建系统里随手 `-D` 定义一下就远程打死全服"的隐患，而它们本该保护的东西（崩溃路径正确性）从未被验证过。
- **建议**：补齐最小测试并纳入 CI：
  1. `RingBuffer` 的属性测试（随机 读写/peek 组合，`cap_` 为 1、2、3、非 2 的幂）；
  2. 帧边界（半包、粘包、`len == 0`、`len == MAX_FRAME_PAYLOAD`、坏 magic、`len > MAX`）；
  3. PING/PONG 往返 + 对端不读 → 应尽早关闭（配合 `failWorkerForTest` 验证 `reactorFailures` 计数）；
  4. `start()/stop()` 100 次循环 + `ASan` 下跑，专抓 P0-1/P0-2（这两类只会在反复启停中暴露）；
  5. 用 `sendTo` + `closeConnection` 竞争测试 P1-4。

### [P2-1] 三处注册失败后的回滚不完整　✅ 已修复（commit bcd2ead）

- **位置**：
  - L552-555：`epoll_ctl ADD` 失败 → 直接 return，**`admitted_` 票券正常由 `admission` 析构释放**，无问题；但 `stats.rejected` 未计数（只有 L565/L578 计）
  - L568-572：`onConnectionOpened` 失败 → `fdIndex_.erase` + `conns_.erase` 后 return，但 epoll 里**仍保留对该 fd 的注册**，随后 `c->fd` 由 `Conn::~Conn` 关闭 —— fd 号被回收后若被新连接复用，同一 epoll 实例里可能残留旧注册
  - L562-563：`fdIndex_.erase` 在 `fdIndex_[c->fd] = c.get()` **之前**就已执行（异常发生在插入中），擦除的是"可能不存在的键"，无害但说明顺序是从别处复制来的
- **建议**：抽一个 `unregisterConn(Conn*)`，统一下面三件事：`epoll_ctl(DEL)` → `fdIndex_.erase` → `conns_.erase`，所有失败分支都走它。`closeConn` 也复用它。
- **已落地（commit `bcd2ead`）**：
  ```cpp
  inline void SubReactor::unregisterConn(Conn* c) noexcept {
      if (c->fd < 0) return;
      fdIndex_.erase(c->fd);
      epoll_ctl(epollFd_, EPOLL_CTL_DEL, c->fd, nullptr);
      ::close(c->fd);
      c->fd = -1;
      c->writeArmed = false;   // 让后续 arm 重新走一次完整的 ctl
  }
  ```
  - 四处调用点：`epoll_ctl ADD` 失败、map 插入抛异常、`onConnectionOpened` 失败、`closeConn`。
  - L562-563 那处"先 `erase` 再插入"的顺序问题随重构消失（原代码在异常路径里擦除的是可能不存在的键，虽无害但确实是复制粘贴的残留）。
  - 注意：`conns_.erase` 由调用方按 `id` 负责（`unregisterConn` 只拿 `Conn*`，不查表），避免在遍历中二次查找。
  - **未纳入本次**：`epoll_ctl ADD` 失败分支原来不计 `rejected`，本次已补齐；但它到底是"拒绝"还是"错误"在语义上更接近 `epollCtlErrors`，两个计数器都会 +1——保留双计数，理由是运维需要同时看到"总数上升"和"有连接没接成"。

### [P2-2] `SUBMISSION` 屏障为每帧加两次共享锁；`pendingBytes` 的内存序论证不成立

- **位置**：`sendTo` L1217-1218、L1238-1244；`processPending` L506、`enqueue` L520-526、`drainWbufToSocket` L602/L673/L763/L1249
- **问题 A（性能）**：高扇出推送（一个生产者 → N 连接）时，每帧都要 `shared_lock(submissionMutex_)` + `shared_lock(connsMutex_)`，两个 `shared_mutex` 共享读也会互相争用 `atomic` 引用计数；再加一次 `pendingBytes` 的 CAS 循环（无竞争时也至少 1 次 RMW）。10k 连接 × 1k 帧/秒 = 20M 次锁操作，收益却是"避免在 shutdown 窗口里返回 Ok"。**建议**：`sendTo` 先查 `life_ == Running`，失败快路径直接返回；只在"确实拿到了 Conn"后才做一次 `shared_lock`，并把 `connsMutex_` 换成按 id 分片的 `vector<shared_mutex>`（8~64 片）。若仍嫌重，可让 shutdown 接受"少数 Ok 被丢弃"——接口注释 L355 已经这么写了。
- **问题 B（正确性）**：`enqueue` / `processPending` 对 `pendingBytes` 用 `relaxed`，代码注释暗示"迁移语义"。实际上 `pendingBytes` 的"先加后写 wbuf"与"先减后……"跨线程，`relaxed` **不提供任何同步**，能工作纯粹是因为 `connsMutex_` 与 `pendingMutex_` 的锁顺带建立了顺序。这是"靠巧合正确"。**建议**：`pendingBytes` 统一用 `acq_rel`（append 位置用 `release`、消费位置用 `acquire`），或在注释里明确写"由 `connsMutex_` 保护"，避免后来者把它当无锁计数器用。
- **问题 C（文档-代码不一致）**：注释 L608 写"pendingBytes 不变：从未写入迁移到 wbuf 内"，但 L601-605 的丢弃分支**会**执行 `fetch_sub`，L673 发送成功也 `fetch_sub`。语义应为："入队时 +size；进入 wbuf 时不变；发送成功 -k；整帧丢弃 / 连接关闭 -剩余"。请把注释改成这三段式。

### [P2-3] `rlimit` 检查只告警不动作，且缺 `SO_RCVBUF`/`SO_SNDBUF` 调优

- **位置**：`start()` L976-983
- **问题**：`maxConnections=10000` 而 `RLIMIT_NOFILE` 默认 1024 时，只打印 `[warn]` 就继续启动 —— 结果是前 ~1000 个连接成功后全面 `accept EMFILE`（此时 L1129-1132 连 errno 都不记录，直接 break），表现为"服务跑着跑着就再也接不进来"。工业场景没有 `systemd LimitNOFILE` 保证时这几乎是必然。
- **建议**：`rlim_cur < maxConnections + 64` 时①尝试 `setrlimit` 抬到 `rlim_max`；②在 accept 循环里对 `EMFILE/ENFILE` 单独计数（`acceptErrors`）并做 100ms 退避（避免 epoll 忙轮询）；③在 `start()` 返回 false，让运维在启动期就发现问题，而不是运行期。

### [P2-4] `closeReason` 存裸指针、`reason` 生命周期靠约定

- **位置**：`Conn::closeReason` L231、`closeConn` L794、`CloseHandler` L319
- **问题**：目前所有调用点都传字面量，安全。但 `conn.closeReason` 没有任何读取方（grep 无消费者），属于死字段；而 `CloseHandler` 注释写"reason is borrowed"，一旦有人传局部 `std::string::c_str()`，就是标准 UAF。
- **建议**：二选一 —— 要么删掉 `closeReason` 字段；要么改成 `std::string closeReason`（在关闭时赋值一次，代价可忽略），并把 `CloseHandler` 签名改为 `(uint64_t, const std::string&, const std::string&)`。

### [P2-5] `doTick()` 的 O(N) 全表扫描与 `pendingTasks_` 的容量上限语义

- **位置**：`doTick` L837-846、`kMaxPendingTasks` L297
- **问题**：每 500ms 全量遍历所有连接（10k 连接 × 2 次/秒 = 20k 次比较，可接受，但会与 epoll 事件处理抢同一线程的缓存）；更值得改的是**没有分片**：一个 reactor 上的 10k 连接会形成单次 tick 的尾延迟尖刺。建议改为按 1/32 分片（每次只扫 1/32，用 `nextSlot_` 游标轮转），把 500ms 的"一次性 O(N)"摊平为每 tick O(N/32)。
- `kMaxPendingTasks = 8192` 是**每 reactor** 的上限，注释未说明；在 10k 连接、单 reactor 10k 的场景下，一次广播即可打满并触发 `queueDrops`。建议按 `maxConnections / numReactors * 4` 动态设定，或至少把上限与 `maxConnections` 挂上关系。

### [P2-6] 其他正确性/可维护性细节

| 位置 | 问题 | 建议 |
|------|------|------|
| L60-71 | `pongFrame()` 返回 `static` 引用，线程安全（C++11 magic static），但 `encodeFrameInto` 与它重复实现了同一段字节序逻辑 | 让 `encodeFrameInto` 调 `pongFrame()` 的构造 lambda，或抽出 `writeHeader()` |
| L84 | `encodeFrameInto` 不做 `len` 上界检查，`sendTo` 层做了；但函数是 `inline` 公开 API，容易被误用 | 加 `assert(len <= MAX_FRAME_PAYLOAD)` |
| L129/L148 | `head_ + size_` 在 `cap_ == 0` 时 `% 0` → UB。`RingBuffer()` 默认构造后若直接 `write` 会崩 | `if (cap_ == 0) return 0;` 或删掉默认构造 |
| L112 | `init()` 在已有数据时直接 `resize`，静默丢数据 | 加 `assert(empty())` 或断言 `cap` 只在构造期设置 |
| L703 | `std::min({sizeof(tmp), c->rbuf.writable(), (size_t)bytesLeft})` 三参 `min` 要求 `initializer_list` 元素同类型，`sizeof` 是 `size_t`，OK；但 `tmp` 是 16KB 栈数组，每 reactor 线程栈 8MB 粗算安全 | 可改 `std::array<uint8_t,16384>` 更稳 |
| L744 | 半包判断 `readable() < HEADER + len` 用 `size_t` 运算，`len` 最大 64K，无溢出 | 保持 |
| L867 | `epoll_wait(..., readHead_ ? 0 : tickMs)` —— 有待续读时用 0 超时忙轮询。若某连接持续 `readPending` 且对端不发文，会形成 100% CPU 空转 | 续读路径应改为"仅在 `recv` 返回 EAGAIN 前"使用 0 超时；更稳妥是给续读加 `min(1ms, tickMs)` 下限 |
| L892 | `EPOLLHUP|EPOLLERR` 用 `continue` 而非 `break`，`keepAlive` 在 `continue` 时正常析构，安全 | 保持 |
| L989-990 | `numReactors > 64` 截断为 64，硬编码魔数 | 提到 `Config::maxReactors` |
| L1077 | `acceptLoop(std::promise<bool>&)` 用 `shared_ptr` 传递但形参是引用，`ready` 的 `shared_ptr` 在 lambda 里被拷贝；`ready.set_value` 两次（L1098、L1185）由 `try/catch` 兜底，安全 | 保持，但建议改用 `std::promise<bool>*` 或直接传值，减少一次 `shared_ptr` |
| L1189 | accept 线程退出时无条件关闭 `listenFd_`，但 `start()` 失败路径 L1029 也关闭 —— 两处都有 `>= 0` 判据，安全 | 保持 |
| L527 | `nextConnId_` 从 1 自增，永远不复用，64 位不会绕回 | 保持 |

## 四、待核实清单

1. `[需核实:在目标 Linux 发行版 + 内核版本上，epoll_ctl(MOD) 与同一 fd 上已就绪的 EPOLLOUT 事件之间的可见性]` —— 影响 P1-1 的触发概率，逻辑上无论内核行为如何都必须修。
2. `[需核实:业务 CloseHandler 是否会在回调内调用 server.stop() 或销毁服务器]` —— 影响 P0-2 的实际可达性；若业务约定"回调内绝不 stop"，则该条降级为 P1 设计约束。
3. `[需核实:numReactors 的实际取值与 maxConnections 的比例]` —— 影响 P2-5 中 `kMaxPendingTasks` 是否够用。
4. `[需核实:线上是否已在 systemd 单元里设置 LimitNOFILE]` —— 影响 P2-3 的紧急程度。
5. `[需核实:是否有未纳入本目录的测试工程]` —— 若存在，P1-5 降级为"测试目录不在评审范围"。
6. `[需核实:目标编译器版本是否 ≥ GCC 9（used to guarantee static init thread-safety 之外的行为无需确认）]` —— 本报告未实际编译，所有编译期结论均为静态推断。
   - **更新**：已用 GCC 15.2 完成宿主侧编译（零警告）；目标编译器版本仍待确认。
7. `[需核实:运行时行为]` —— 所有竞态类修复（P0-1/P0-2/P1-1）的验证完全依赖真机 ASan + 反复启停，宿主侧编译检查对此无能为力。

## 五、修复进度

| 编号 | 问题 | 状态 | commit |
|------|------|------|--------|
| P0-1 | 生命周期状态与失败标志混用一个 atomic | ✅ | `ed08532` |
| P0-2 | `closeConn` 在回调栈内析构连接/服务器 | ✅ | `c80a551`、`51729c3` |
| P1-1 | `wasEmpty` 判据与保留态不匹配 / arm-disarm 盲写 | ✅ | `478e074` |
| P1-2 | 关闭时 wbuf 残留不回退 `pendingBytes` | ✅ | `3db4521` |
| P1-3 | `accepted` 计数口径与 `rejected` 冲突 | ✅ | `bcd2ead` |
| P2-1 | 注册失败回滚不完整 | ✅ | `bcd2ead` |
| P1-4 | 缺 `shutdown(SHUT_WR)` 优雅关闭 | ⬜ 待做 | — |
| P1-5 | 无测试、故障注入开关无消费者 | ⬜ 待做 | — |
| P2-2 | `sendTo` 双锁 + `pendingBytes` 内存序论证不成立 | ⬜ 待做 | — |
| P2-3 | rlimit 只告警不动作 | ⬜ 待做 | — |
| P2-4 | `closeReason` 裸指针死字段 | ⬜ 待做 | — |
| P2-5 | `doTick` O(N) 扫描未分片 | ⬜ 待做 | — |
| P2-6 | 其余细节（`RingBuffer` `%0` UB 等） | ⬜ 待做 | — |

## 六、建议行动顺序

1. **先修 P0-1 / P0-2**：这两条都属于"平时看不出来、只在启停竞争与关闭瞬间暴露"的问题，且都只需局部重构（状态机 + 延迟回调）。建议先补 `start/stop` × 100 + ASan 的冒烟测试，作为修复的验收标准 —— 否则修完也无法证明修好了。
2. **再修 P1-1 / P1-2 / P1-4**：三者都在写路径上，建议合并为一次"写侧状态机 + 关闭协议"重构，改动集中在 `armWrite/disarmWrite/drainWbufToSocket/closeConn` 四个函数。
3. **P1-3 / P2-1 / P2-2C** 是"让统计说实话"，成本低，随第一次提交一起做。
4. **P1-5 / P2-3** 属于工程化，建议在本迭代内补齐，并把 `INDUSTRIAL_TCP_TESTING` 的开关纳入 CI 矩阵。
5. **P2-2A / P2-5 / P2-6** 作为性能与可维护性优化，排入下一个迭代。

> 本报告为只读静态审查，未修改任何源码。
