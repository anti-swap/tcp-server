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

## 四种假阴性（按发现顺序）

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
