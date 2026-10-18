# verify/ — 宿主侧编译检查

这个目录只做一件事：**在不具备 Linux 工具链的机器上，让 `tcp-server-mt.h` 真正被编译器解析一遍**。

```bash
bash verify/build.sh      # 0=干净, 1=有诊断, 2=缺工具链
```

## 能验证什么

- 语法、类型、模板实例化、`const` 正确性
- `-Wall -Wextra -Wpedantic -Wshadow` 下的全部警告
- 头文件被两个翻译单元包含时的 ODR/重复定义问题

## 不能验证什么

- **任何运行时行为**：没有链接、没有执行，`epoll_wait` 的时序、竞态、回调重入都测不到
- 真实 Linux 头文件差异：`stub/` 是按 POSIX 语义手写的声明层
- 内存错误：需要 Linux 上的 `-fsanitize=address,undefined`

真机验收仍需：

```bash
g++ -std=c++17 -Wall -Wextra -g -fsanitize=address,undefined \
    -DINDUSTRIAL_TCP_TESTING test.cpp -pthread -o test && ./test
```

## 实现要点（两个坑）

1. **`cc1plus` 需要 `ucrt64/bin` 在 PATH 上。**
   只用绝对路径调用 `g++.exe` 时，`cc1plus.exe` 找不到自己的 DLL，进程直接退出码 1，
   **且不产生任何诊断信息**——看上去和「编译干净」一模一样。`build.sh` 显式导出了 PATH。
   本项目的编译检查最初就在这里拿到了假阴性。

2. **`g++` 是原生 Windows 程序，不认 `/c/...`。**
   路径必须转成 `C:/...`，`build.sh` 用 `cygpath -m` 处理。

## 检查器自身的可信度

添加时用注入法验证过：往头文件里插一个未声明标识符 → 报出精确行号并 exit 1；
移除后 → exit 0。不做这一步的话，「通过」这个结论本身就没有意义。

**2026-10-17 重新验证时踩到同一个坑的第二种形态**：把探针追加到文件**末尾**（`} // namespace industrial`
之后）时，`build.sh` 依然报 OK。原因不是编译器，而是探针在命名空间外部、又只被 `inline`
修饰——没有调用者就不会生成代码，语法与名字查找都没被触发。**注入必须在命名空间内部，
且最好直接在已有函数体里引用未声明符号**，否则会得到另一个假阴性。

**第三种形态：流水线本身没在编译。** 宿主 shell 的 PATH 会被上层包装脚本弄坏（`ls` / `tr` / `head`
全部 command not found），此时 `build.sh` 会先走到「no C++ compiler」分支——这一层还算诚实，给了 exit 2。
但一旦为了绕开它而换用别的调用方式，“输出为空”和“零诊断”就长得一模一样，
而屏幕上照样没有报错。**做法：只要调用方式变了、或距上次成功过了很久，
就先编译一个故意写错的对照文件**，确认它报出精确行号且 exit 1。
这一轮正是因为先跑了对照，才敢相信头文件那两个 rc=0 是真的。

## 在 WSL 里跑全套

```bash
bash verify/run-in-wsl.sh                      # 默认配置（2 / 1 reactor 各一轮 + 故障注入）
ENABLE_ASAN=1 bash verify/run-in-wsl.sh        # 另跑一轮 ASan + UBSan
ENABLE_TSAN=1 bash verify/run-in-wsl.sh        # 另跑一轮 TSan
ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run-in-wsl.sh
```

Windows 路径走 drvfs 直接访问，不必把项目复制进 WSL 文件系统。三个配置是**分别**构建的：
ASan 与 TSan 不能链进同一个二进制，而且两者都慢到足以让人不再愿意跑第三个。

## `close_semantics.cpp` —— 断言线上可见的关闭行为

`close_semantics.cpp` 与 `compile_check.cpp` 不同：它断言的是**线上可见的行为**，编译器看不见。

```bash
# 只能在 Linux 上跑
g++ -std=c++17 -Wall -Wextra -pthread verify/close_semantics.cpp -o close_semantics && ./close_semantics

CS_REACTORS=1 ./close_semantics     # 所有客户端落到同一个 reactor
```

`CS_REACTORS` 不是可有可无的旋钮：`pickReactor()` 是轮转分配，多于一个 worker 时，**只发生在某一个
worker 上的故障会被平均掉** —— 两个 reactor 时表现为"每隔一条连接死一次"。

| 用例 | 断言 |
|------|------|
| A | `sendTo` 应答 + `closeConnection`（"先答再挂"）→ 应答完整收到，结束于 FIN 而非 RST |
| B | 关闭时服务端尚有未读请求 → 不得退化为 RST，且请求不被静默丢弃 |
| C | 坏 magic → 对端观察到 RST，`parseErrors` / `aborted` 计数 |
| D | `accepted == closed`，无连接泄漏 |

> **首轮跑就抓了个 P0。** `accepted=0`、所有 `connect()` 直接失败：accept 线程在 `Running` 发布之前
> 就判定自己该退出，循环一次都没进，顺手把监听套接字关了（CODE-REVIEW.md 的 P0-3）。静态审查没看出来，
> 因为它取决于调度器先唤谁 —— 只有真机跑才会露头。这条路值得维护。
>
> 之后同一台机器上又抓到三个（P0-4 / P0-5 / P0-6），全都在**关闭路径与启动发布**上。

## `fault_injection.cpp` —— 让 `INDUSTRIAL_TCP_TESTING` 有消费者

`failWorkerForTest` / `failAcceptForTest` 是为"worker 从异常里退出以后该怎么办"准备的，
但在补这个文件之前，**没有任何代码翻动它们**：两个开关，零个消费者。
没人去扳的开关，和被注释掉的断言一样，都不是测试。

```bash
g++ -std=c++17 -Wall -Wextra -pthread -DINDUSTRIAL_TCP_TESTING \
    verify/fault_injection.cpp -o fault_injection && ./fault_injection
```

没有那个宏时这个文件**编译不过**（`#error` 而不是静默少测一个用例——见下）。

| 用例 | 断言 |
|------|------|
| A | reactor 异常退出 → `isRunning()` 转假、`reactorFailures` 计数、`stop()` 仍能正常返回 |
| B | accept 线程异常退出 → 同上，且它把持的监听套接字被真正释放 |
| C | 12 轮「启动 → 注入故障 → stop → 再启动」→ 每次都留下一个还能重新起来的服务器 |

`stop()` 一律包在带超时的 `within()` 里跑：恢复路径死锁时的表现只是"测试跑得久"，
而退出码分不清那和一台慢机器有什么区别。

## `backpressure.cpp` —— 一次广播排得下吗

`kMaxPendingTasks` 曾经是每 reactor 恒定的 8192。它是一个队列上限，但服务器长大之后就变成了
**扇出上限**：一次广播填满队列，之后的全被计入 `queueDrops`，而这个计数向运维说的是"对端没收"。

用例做两件事来保证自己量到的是那一条机制：把 `maxPendingBytes` 和 `wbufCapacity` 都抬到不可达
（三条拒帧路径里只有被测量的那条能触发），再把 worker 扣在 open 回调里（队列只有在没人排空它时
才会填满）。修复时它是 **红 → 绿** 的：

```
修复前   queued 8192 of 9000 (808 refused), queueDrops=808
修复后   queued 9000 of 9000 (0 refused),  queueDrops=0
```

8192 一帧不差 —— 卡住的位置正好是那条常量。它是故意先红着提交的（`f8a6523`）。

## 七种假阴性（按发现顺序）

> 更新：现在是八种。第 8 条是关于 **cpplint 的退出码** —— 见本节末尾。

前面三形态是**检查器自身**失效；后面几条是**测试自身**在骗人，更隐蔽，因为它们有输出、有绿色。

1. **`cc1plus` 的 DLL 不在 PATH** → 编译器退出码 1 且零诊断，看上去和"干净"一模一样。
2. **探针追加到文件末尾** → 探针在命名空间外、只被 `inline` 修饰，没有调用者就不生成代码，
   名字查找都没被触发，`build.sh` 照样报 OK。注入必须在命名空间内部、最好直接在已有函数体里。
3. **流水线本身没在编译** → 宿主 shell 的 PATH 被上层包装脚本弄坏（`ls` / `tr` / `head` 全部
   command not found）时输出为空，而"输出为空"和"零诊断"长得一样。做法：只要调用方式变了、
   或距上次成功过了很久，先编译一个故意写错的**对照文件**，确认它报出精确行号且 exit 1。
4. **测试抓错了连接。** B 用例用 `connectionIds().front()` 取 id，而当时 `conns_` 里有一条本该
   消失的连接 —— 于是它关了**错误**的套接字，下面每条断言都在对一个没人测试的连接通过。
   本该抓到丢失应答的用例，正在丢应答的同时报告成功（P0-6 之所以藏了那么久）。现在：
   记录连接前的 id 集合，只认可连接之后新出现的那个。
5. **把"干净的 FIN"当成"没有 RST"。** 旧版 `readToEnd` 只有一个 `reset` 布尔量，
   FIN、超时（什么都没到）、无效描述符全都折叠成 `false`。于是"服务端没中止"和
   "服务端中止了但内核拒绝发 RST"给出同一个答案。现在返回三态并打印。
6. **时序决定成败。** B 用例先写两个请求再立刻 `closeConnection`，请求到达与否取决于网络栈，
   约五把里输一把。现在等 `bytesRecv` —— 把用例锚定到它真正要测的那一刻（字节已抵达、
   reactor 是否读过仍未定），而不是把断言放松。
7. **退出码没有过桥。** 本套件的每条结论都读自 `$?`。但把命令以内联字符串的形式交给 Linux
   （`wsl.exe -- bash -lc 'false; echo $?'`）时，`$?` **恒为 0**：`(exit 3)`、一个编译出来
   `return 7` 的程序，全都报 0。于是走这条路的脚本会输出"全部通过"，而这里的"通过"与"失败"
   长得完全一样 —— 这是最难挽回的一种形态，因为它连"有异常发生"这个信号都不给。
   走**脚本文件**（`bash verify/run-in-wsl.sh`）退出码是完好的，本套件一直走这条路，以往结论不受影响。
   现在这件事本身成了断言：`verify/probe_exit_code.sh` 最先跑，把管道不成立的情况直接判死，
   并说明它问了什么。
8. **cpplint 无论有没有发现东西都返回 0。** 实测：一个只有 `int x = (size_t)y;` 的文件会打印
   `Total errors found: 2`，退出码照样是 0。所以结论只能从输出里解析，问 `$?` 等于让这个检查
   永远通过。更细的一层：`Total errors found:` 这行**只在非零时打印**，文件干净时除
   `Done processing` 外什么都不输出 —— 于是"干净"和"根本没跑"的输出长得也一样。
   因此 `run-in-wsl.sh` 每次先 lint 一个必定报错的对照文件，它不报错就不接受后面的"干净"结果。

## 一处 TSan 误报（此机器上）

`backpressure.cpp` 要扣住 worker，最自然的写法是 mutex + `condition_variable`。本机（WSL1 /
Ubuntu 的 g++ 11.4）的 ThreadSanitizer 会对此报 **double lock of a mutex**，紧接着报"同时被
这把锁保护着"的标志量有 data race —— 而 **`verify/probe_tsan_condvar.sh` 里那段不含服务端、
不含套接字的标准握手，报的是完全一样的两种错**。也就是说这是库/平台的性质。

处理方式不是加 suppression：那等于让代码库永久地与 sanitizer 争执。改成在原子量上轮询并带
20 秒上限 —— 要测的性质（worker 被扣住、队列没人排空）一点没变，可被误建模的构件没有了。
改完之后 TSan 配置下该用例 `rc=0`。

> 这里有一条更一般的教训：**sanitizer 报错时先做最小复现，不要先改代码**。上一节列的七条是
> "明明错了却报绿"，这一条是它的镜像 —— "明明没错却报红"。两者都能把人带到完全错误的方向。

## 一处无法断言的地方（不是缺陷）

本机内核（WSL1，4.4.0-19041-Microsoft）**不把 `SO_LINGER{on,0}` 变成 RST**：用一对裸 socket
验证过，设了零 linger 再 close，对端收到的是干净 FIN。这是机器的性质，不是服务端的。

所以 C 用例会先自己问内核一次（`kernelHonoursZeroLinger()`），把结论打印出来：支持就断言 RST，
不支持就打印 `[SKIP]` 并改为断言服务端侧的证据（`parseErrors` / `aborted`）。**跳过会写明理由，
不等于通过。** 换到真正的 Linux 内核上，那条断言自动恢复生效。
`build.sh` 会**编译**（不链接、不运行）这个文件，用于捕捉公开接口漂移——例如 `closeConnection`
改名这类 `compile_check.cpp` 完全发现不了的问题。为此还需要 `stub/posix_shim.h` 提供
`sockaddr_in` / `htons` / `send` / `getsockname` / `SO_RCVTIMEO` 等测试端要用的声明。

## stub 的边界

`stub/posix_shim.h` 只声明类型和函数原型，不提供实现。刻意保守：

- 常量值按 Linux 实际取值（`EPOLL_CTL_*`、`MSG_NOSIGNAL` 等）
- `epoll_data_t` 用 `union`，与 glibc 一致
- 不模拟 `errno` 语义，所以依赖错误码分支的逻辑只能靠 review

它**不是**兼容层，不要试图用它跑程序。

## 一个概率型缺陷怎么测：case F 与插桩工具

`close_semantics` 的 F 用例（20 轮"一知道请求到了就关"）针对的窗口只有几微秒：请求已被处理，
但它产生的应答任务还没执行，此时 close 落进来会马上 park，跟在它后面的应答任务被 `closing`
拒绝。单次跑几乎必过——跑一遍看到 `0 of 20` 不代表没问题。

因此：

- **评估只能看复现率**：`bash verify/evidence/loop_close_race.sh 8 <修复前的 ref>`，
  两份构建各跑 N 遍 TSan 并统计。本机实测修复前 4/8 复现、修复后 0/8。
- **定位靠插桩不靠猜**：`python3 verify/evidence/trace_close_race.py <in> <out>` 给 header
  **副本**打上决策点日志（`SUBMIT_SEND` / `SUBMIT_CLOSE` / `TASK_SEND` / `PARK_IMMEDIATE`
  / `PARK_DEFER` / `REFUSE`），出错那轮的顺序是一条 reading-list：

```
[t] SUBMIT_SEND id=22     handler 排入应答 A
[t] SUBMIT_CLOSE id=22    应用的关闭插在 A 与 B 之间
[t] SUBMIT_SEND id=22     handler 排入应答 B
[t] TASK_SEND id=22       A 执行 → 放行
[t] PARK_IMMEDIATE id=22  关闭任务发现已无可读 → 立刻 park
[t] TASK_SEND id=22 → REFUSE  B 撞上 closing
```

前三个假设都是错的，trace 一行就给出答案。**但注意 tracing 会改变被测窗口的时序**，所以
A/B 对照必须用两份都未插桩的构建——`loop_close_race.sh` 就是这么做的。

可复用判据：判断"这条连接还欠字节吗"要看 `pendingBytes > 0`（已占额度、还没进 wbuf），
而不是 `wbuf.empty()`。handler 的应答是 `submitSend` 排的任务，从不直写 wbuf。

## 怎么给一个"不会发生的危险"做证据

有些隐患今天不可达，所以没法写一个会失败的用例——但它们才是将来某次重构之后最贵的东西。
两个已落地的处理办法：

**（一）注入：把不变量打破，让危险变成真的。**

`doTick` 的迭代安全原本依赖"若 `finSent` 则必 `closing`"这条跨 5 个函数、没有注释也没有断言的
不变量。`verify/evidence/inject_fin_without_closing.py` 把 `finSent && !closing` 的状态直接摆到
`doTick` 眼前——这正是任何一次只改了其中一处、漏了另一处的重构会造出来的样子。

```
bash verify/evidence/check_doTick_invariant.sh f27cfc1^
   before  AddressSanitizer: heap-use-after-free   rc=1
   after   accepted=24 closed=24 stillOpen=0       rc=0
```

两侧都注入，唯一变量是被测的 `doTick`。**必须显式给 before 参数**：默认 HEAD 的话，修复一落地
脚本就在拿修复跟修复自己比较，还打印一个自信满满的结论。

**（二）计数：让"不可达"保持可测量，而不是停留在判断。**

`ServerStats::epipeWrites` 数 drain 撞上 EPIPE 的次数。目前五种"让对端带着未读数据消失"的构造
（`verify/evidence/probe_epipe.cpp`：SO_LINGER 0、直接 close、带未读数据 close、先半关闭…）
实测都是 0。这不等于不可达，只等于"没触发成功"——计数器的意义就是有朝一日它动起来时，能让那个
差别被看见，而不是等到有人报告"连接不释放"。

注意本机内核把 `SO_LINGER{on,0}` 变成 FIN 而不是 RST（`verify/evidence/so_linger_probe.cpp`），
所以想在这里造真 RST 比看起来困难；`eofReads=1` 则说明五种场景里对端 FIN 都被正常识别，没有读自旋。
