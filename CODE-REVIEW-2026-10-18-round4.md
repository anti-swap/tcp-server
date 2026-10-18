# 第四轮：外部审查核实（`finishPeerClosed` 的停靠 MOD 失败）

本轮不是一次完整复审，只有一条：用户带来一张外部审查截图，断言
`finishPeerClosed` 的“停靠”分支在 `epoll_ctl(MOD)` 失败后只加计数、不拆除，
会导致 busy loop，并称“前两轮都说过，这版还没改”。

结论：**断言成立，已修复（`0666ba8`）**。截图对因果链的描述与代码逐行吻合；
但对“前两轮都说过”这一点需要更正，且它漏掉了一种后果相反的失败模式。

---

## 1. 代码事实

`tcp-server-mt.h` `finishPeerClosed()`，对端 FIN 且 `wbuf` 非空时：

```cpp
if (epoll_ctl(epollFd_, EPOLL_CTL_MOD, c->fd, &wev) == 0)   // wev = EPOLLOUT only
    c->writeArmed = true;
else
    stats.epollCtlErrors++;
// A failed MOD keeps EPOLLIN armed: the drain spins, nothing breaks.
```

LT 模式下对端 FIN 让 socket 永久可读，`EPOLLIN` 未摘 → 每次 `epoll_wait` 立刻返回
→ `handleRead` → `recv`=0 → `finishPeerClosed` → 同一个 MOD 再失败。一次
`epoll_wait` 一轮，直到某个截止条件到来。

## 2. 复现手段

`verify/evidence/probe_fin_mod_failure.cpp`，`-Wl,--wrap=epoll_ctl`，只拦一种调用形状：
`MOD && (events & EPOLLOUT) && !(events & EPOLLIN)`（`armWrite` 是 IN\|OUT，
`disarmWrite` 是 IN，故不会误伤健康调用）。负载：16MB 应答 + 对端 `SO_RCVBUF=2048`
且不读，250ms 后 `shutdown(SHUT_WR)` 并保持 socket 打开（这样不会走 RST 路径）。

## 3. 红 → 绿

| 注入 | 指标 | 修复前 | 修复后 |
|---|---|---|---|
| 不失败（对照） | `eofReads` / CPU | +1 / 0% | +1 / 0% |
| MOD 返回 ENOMEM | `eofReads` / `epollCtlErrors` / CPU | **+67835 / +67835 / 97%** | +1 / +1 / 0% |
| MOD 返回 ENOMEM | 对端补齐读取后拿到的字节 | 16781312 / 16781312，但白烧一核 | 4096，连接已拆除（`aborted=1`） |
| MOD 返回 ENOENT | 对端补齐读取后拿到的字节 | **3145728 / 16781312，随后挂死** | **16781312 / 16781312** |
| MOD 返回 ENOMEM（完整周期，idle 1.5s + linger 0.5s） | 失败调用次数 / 结局 | 314903 → idle 超时 → 仍停靠 → linger 强拆，`bytesDropped=13635584` | 1 → 就地拆除，`idleTimeouts=0` |

修复前后各跑一次，量级稳定（三次数值 59775 / 67078 / 67835）。

## 4. 修复（`0666ba8`）

与 `armWrite` 完全同构——同一个系统调用，此前文件里已有两个地方给出裁决，
只有这里选了宽松策略：

- `ENOENT`：用 `ADD` 收养 `EPOLLOUT`（`writeArmed = true`）。这不是可选的补救，
  而是必需：`ENOENT` 意味着 fd 不在任何兴趣表里，不收养就是永久静默。
- 其余错误：`epollCtlErrors++` + `closeConn(c, "epoll park registration failed", false)`。

没有第三种选择：半注册成功的停靠唯一能做的事就是自旋。

**取舍，写在这里而不是留在注释里：** 非零错误下的拆除会丢掉仍未发出的字节
（本次 13.6MB）。备选方案是 `DEL` 掉 fd 换掉自旋、再由一个停靠重试集合在
`doTick` 里反复重试注册——它需要新增状态，且与 `armWrite`/`disarmWrite` 对同一
系统调用的裁决相反。在 ENOMEM 级别的触发概率下，选择一致性。若将来
`epollCtlErrors` 在生产里真的动过，这个取舍就是第一个该复查的地方。

## 5. 对截图的三处更正/补充

1. **“前两轮都说过”不准确。** 前两轮只把 MOD **成功**路径列为条目并修复
   （P1-1b / `a297d9b`）；失败分支从未成条目，`nothing breaks` 那句注释正是那次
   修复时写下的。所以“这版还没改”成立，“说过”只在 P1-1b 这个主题层面成立。
2. **“busy loop 直到 linger/idle 超时”低估了。** 完整周期是两段计时叠加：
   idle 超时触发优雅关闭后连接**仍在停靠**（MOD 继续失败），要再等 `closeLingerMs`
   才被强拆；自旋贯穿两段。
3. **截图只说自旋，漏了 ENOENT 的静默。** 见上表第 4 行：修复前对端只能拿到
   3MB 中的已发送部分然后挂死，这是比自旋更严重的一种（数据不到，且谁也不会发现）。
   截图的建议“ENOENT 可以用 ADD 恢复，其他错误必须 abort”是对的，但没说清
   ENOENT 的后果方向相反——它不是“也自旋”，而是“全静默”。

## 6. 附带发现（未在本提交处理）

`verify/build.sh`（Windows 侧编译检查）在 HEAD `9c8200a` 上已经失败：
`verify/stub` 缺 `RLIM_INFINITY` 与 `setrlimit`，而 `start()` 里的
`RLIMIT_NOFILE` 提升（`9c8200a` 之前引入）用到了它们。与本条缺陷无关，
单独提交处理。

## 7. 回归

`bash verify/run-in-wsl.sh`，`ENABLE_ASAN=1 ENABLE_TSAN=1`：

- plain / asan+ubsan / tsan × { close_semantics(默认, CS_REACTORS=1), backpressure,
  fault_injection } = 12 种配置，`rc=0`
- cpplint：对照文件 2 findings（证明该检查可失败），`tcp-server-mt.h` 0 findings
- 退出码哨兵 9 项全 OK
