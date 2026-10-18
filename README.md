# tcp-server

单头文件形式的 Linux TCP 服务端：C++17、epoll 水平触发、多线程多 reactor，
全部实现都在这一个头文件里。

```cpp
#include "tcp-server-mt.h"          // -I<repo>/include  -std=c++17  -pthread
industrial::MultiReactorServer server(cfg);
```

没有 `.cpp`，没有构建脚本，没有第三方依赖。要用就把 `include/tcp-server-mt.h`
复制进自己的工程。它需要 Linux（`epoll` + `eventfd`），所以在本机 Windows 上
无法链接出可执行文件——见下面"验证"一节说的两套检查。

## 它长什么样

- **一个 accept 线程 + N 个 sub-reactor**，每个 reactor 跑在自己的线程上，各自持有
  自己那份连接。落在别的线程上的提交走每 reactor 一个队列，加 `eventfd` 唤醒。
- **关闭是分阶段的**，不是 `close(fd)`。一个连接先被标记 `closing`，把已经排好队的
  应答发完，再 FIN。这部分语义是八轮审查里改动最多、探针最密的地方。
- **计数器在 `ServerStats` 里**，每个都写了它什么时候动、以及哪些分支本应到它却到
  不了。读那一段注释比读这里的概述有用。

## 验证

这个项目没有"随便跑一下试试"的位置，两套检查各管一段：

```bash
# Windows/MSYS 侧：把头文件真正交给编译器解析一遍。21 个翻译单元，不链接、不执行。
bash verify/build.sh

# Linux/WSL 侧：真正跑起来。7 个套件 × {plain, asan+ubsan, tsan} = 21 个配置，含 cpplint。
wsl env ENABLE_ASAN=1 ENABLE_TSAN=1 bash verify/run-in-wsl.sh
```

环境变量必须走 `wsl env`，写成 `ENABLE_ASAN=1 wsl ...` 会静默失效——那样你会拿到
一份只跑了 plain 却看起来全绿的输出。这是个踩过的坑，不是风格问题。

`build.sh` 能抓住的是类型、模板、`const` 正确性和所有警告；抓不住任何运行时行为。
真正跑行为的是 `run-in-wsl.sh` 里的竞态、SAN 和语义用例。两者都不能省。

## 目录

```
include/tcp-server-mt.h   唯一的交付物，2937 行
verify/                   编译检查 + 运行套件 + 一次性证据探针
docs/review/              八轮审查记录，含索引
CPPLINT.cfg               80 列；只剩版权过滤一件事在做
```

`verify/evidence/` 里那二十个探针不是常跑的测试，是当时为某一条断言写的取证程序，
各自对着一段已经被修复的历史。其中两个要按 git 历史取头文件，所以它们对 2026-10-18
那次目录整理前后的两个位置都做了兼容。

## 仍然开放

三轮（第六、七、八轮）都带着同一批没能自己关闭的核实项。列在这里是因为它们
没有被否认，只是没在本机被执行过：

- **真内核上的 `EPOLLHUP` / `EPOLLERR` 覆盖**：用例写好了，但本机 WSL1 内核不投递
  这两个事件，所以它恒为 `[SKIP]`。换到真 Linux 内核上会自动生效。
- **多 reactor 下的实测**：几条与 reactor 个数有关的结论只用 `numReactors = 1` 跑过。多 reactor 不改变那些函数的控制流（都只在 owner 线程上跑），
  但确实没有实测。
- **一个混在一起的计数**：某类残余丢弃现在混在 `queueDrops` / `bytesDropped` 里，
  与真正的关闭分支同计数。按现有契约这是同一件事，要分开看得先新增计数器。

## 审查

这个头文件被完整复审过五轮，另有三轮是核实外部提出的问题。每一条缺陷都是先写探针
让它真的出错，再改，再跑同一个探针确认变绿。记录与索引在
[`docs/review/README.md`](docs/review/README.md)。

## 许可

Apache License 2.0，见 [LICENSE](LICENSE)。
