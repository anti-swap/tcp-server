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

## close_semantics.cpp —— 唯一一个真正的运行时测试

`close_semantics.cpp` 与 `compile_check.cpp` 不同：它断言的是**线上可见的行为**，编译器看不见。

```bash
# 只能在 Linux 上跑
g++ -std=c++17 -Wall -Wextra -pthread verify/close_semantics.cpp -o close_semantics && ./close_semantics
```

四个用例（对应 P1-4）：

| 用例 | 断言 |
|------|------|
| A | `sendTo` 应答 + `closeConnection`（"先答再挂"）→ 应答完整收到，结束于 FIN 而非 RST |
| B | 关闭时客户端仍有未读请求 → 不得退化为 RST |
| C | 坏 magic → 对端观察到 RST，`parseErrors` / `aborted` 计数 |
| D | `accepted == closed`，无连接泄漏 |

> **首轮跑就抓了个 P0。** `accepted=0`、所有 `connect()` 直接失败：accept 线程在 `Running` 发布之前
> 就判定自己该退出，循环一次都没进，顺手把监听套接字关了（CODE-REVIEW.md 的 P0-3）。静态审查没看出来，
> 因为它取决于调度器先唤谁 —— 只有真机跑才会露头。这条路值得维护。
`build.sh` 会**编译**（不链接、不运行）这个文件，用于捕捉公开接口漂移——例如 `closeConnection`
改名这类 `compile_check.cpp` 完全发现不了的问题。为此还需要 `stub/posix_shim.h` 提供
`sockaddr_in` / `htons` / `send` / `getsockname` / `SO_RCVTIMEO` 等测试端要用的声明。

## stub 的边界

`stub/posix_shim.h` 只声明类型和函数原型，不提供实现。刻意保守：

- 常量值按 Linux 实际取值（`EPOLL_CTL_*`、`MSG_NOSIGNAL` 等）
- `epoll_data_t` 用 `union`，与 glibc 一致
- 不模拟 `errno` 语义，所以依赖错误码分支的逻辑只能靠 review

它**不是**兼容层，不要试图用它跑程序。
