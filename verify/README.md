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

## stub 的边界

`stub/posix_shim.h` 只声明类型和函数原型，不提供实现。刻意保守：

- 常量值按 Linux 实际取值（`EPOLL_CTL_*`、`MSG_NOSIGNAL` 等）
- `epoll_data_t` 用 `union`，与 glibc 一致
- 不模拟 `errno` 语义，所以依赖错误码分支的逻辑只能靠 review

它**不是**兼容层，不要试图用它跑程序。
