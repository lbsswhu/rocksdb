# ZERO_RANGE 性能测试汇总（2026-11-24 至 2026-11-26）

本批结果没有显示 ZERO_RANGE 在这些配置下有普遍吞吐收益。完整 YCSB 长测中，开启相对关闭的六项汇总吞吐变化为 **-0.19%**，此前 120 秒短测中 B/E 的小幅提升未重现。db_bench 的部分同步写场景有正向信号，但前台收益与包含收尾工作的收益需要分别判断。

## 测试版本与适用范围

- 实际测试提交：`0a47210279850a86ee97e8fc1dae846d4a110db3`。
- 它与本分支第一阶段提交 `cd7a6a37f` 的 Git tree 完全相同：`3137209b0f0686fa248b417dca291718e41c9793`。
- 发布报告时的分支代码基线：`e1915607462d7d4d153ef00cbc248e214a4ab98c`。该提交有后续 WAL 收尾与 Append 优化，**本批结果没有测试该后续实现，不能代表它的性能**。
- db_bench 的 A0 是父提交 `4d57a393a8f1563c0dff1ff468756c23d89e3601`；A1/B 分别为目标提交关闭/开启 ZERO_RANGE。YCSB 两批只比较同一目标提交、同一 jar 的开关效果。

本报告与 [设计文档第 15 节](../../../rocksdb-preallocate.md#157-本轮实现与验收结果) 中的后续优化实验独立，样本没有合并。

## 环境与控制

所有负载、JNI/Maven 构建及冒烟测试都在 `lb_rocksdb_16u` 容器中执行。正式性能测试固定 16 CPU quota、cpuset `0-15`、32 GiB 内存，memory 与 memory+swap 配额均为 `34359738368`，禁用 swap。记录中无 OOM、内存 failcnt 增长或 swap 使用。

db_bench 数据位于 `/home` 的 ext4；YCSB 数据位于 `/data` 的 ext4/NVMe。两种工具的绝对吞吐不直接比较。计时期间不并行运行编译、搜索、磁盘扫描、性能采样器或其他基准；低频进度检查后期绑定在宿主机 CPU16，位于测试 cpuset 之外。db_bench 期间会话恢复时做过一次轻量状态检查。未清空宿主机全局缓存，也未强制隔离宿主机其他进程。

未纳入此前不限内存的探索数据。所有归档都是实际执行记录，保留原始路径和命令；没有将旧日志改写为可移植路径。公开文本副本统一 LF 并去除行尾空格；JSON/CSV 数值和配置内容不变，压缩包内原始日志字节不变。

## db_bench：8 种组合

[完整报告](db-bench-20261124/summary.md) · [逐次 JSON](db-bench-20261124/results.json) · [CSV](db-bench-20261124/results.csv)

只写使用 `fillrandom`；读写使用 `readrandomwriterandom`，50% 读/50% 写。16 前台线程，key/value=16/1024 B，无压缩，batch=1，64 MiB memtable/目标 SST，后台 jobs=2。每配置/版本预热一次 2 秒，正式三次各 5 秒；共 96 次运行，24 次预热不计入下表。

下表比较 **目标提交开启优化 B 相对父提交 A0**，各项为三轮中位数之比：

| 模型 | WAL sync | concurrent memtable write | 前台吞吐变化 | 含最终 compactall/关闭的吞吐变化 |
|---|---:|---:|---:|---:|
| 只写 | false | false | -1.50% | +0.50% |
| 只写 | false | true | -2.25% | -0.96% |
| 只写 | true | false | +11.51% | -5.30% |
| 只写 | true | true | +5.75% | +5.62% |
| 读写 50/50 | false | false | -0.84% | -1.68% |
| 读写 50/50 | false | true | -0.42% | +0.29% |
| 读写 50/50 | true | false | +1.46% | +0.88% |
| 读写 50/50 | true | true | +1.78% | +1.64% |

“含最终 compactall/关闭”包括 DB open、前台工作、额外全库范围的同步 `compactall`、统计输出和 DB close；不包含 seed 复制及离线校验。它不能直接当作正常读写后关闭数据库的性能。同步只写、关闭 concurrent 的前台 +11.51% 没有转化为整体收益。

同一目标提交 B/A1 的对照、配对变化范围及 P99 在完整报告中。每组只有三次 5 秒样本，属于短时探索，不能认定长期稳定收益。96 次离线 SST 校验/扫描通过，32 次开启运行均触发 WAL/flush/compaction 的 ZERO_RANGE，无 fallback 或分配/裁尾错误。

## YCSB：120 秒三轮与完整 1 亿操作

[120 秒报告](ycsb-120s-20261125/summary.md) · [完整长测报告](ycsb-100m-20261126/summary.md)

沿用 `/home/lb/YCSB/test_scripts` 的脚本函数与 RocksDB 配置；两组独立复制同一个已有的 1 亿记录、约 117 GiB 备份库，组内按原脚本顺序运行 A→F。未重新执行数据 load。原 binding 为 WAL 开启、sync=false，concurrent memtable write=true；配置中的 `use_fsync=true` 不等于每次写 `sync=true`。JVM heap 固定 4–8 GiB。

- 短测：每组先只读预热 120 秒，每个 workload 正式运行 120 秒，三轮，轮换开关组执行顺序；共 36 项正式负载和 6 次预热。
- 完整长测：每组先完成 1 亿次只读预热，再依次完成 A–F 各 1 亿次操作；开关各一遍，`maxexecutiontime=0`。12 项正式负载加 2 次预热实际操作数均核对为 `100000000`，合计 14 亿次逻辑操作。总时间为 2026-11-26 09:19:53–11:59:41（UTC+08:00）。

| Workload | 模型 | 120 秒三轮中位数：开启/关闭 | 完整长测关闭 ops/s | 完整长测开启 ops/s | 完整长测：开启/关闭 |
|---|---|---:|---:|---:|---:|
| A | 50% 读 / 50% 更新 | -1.37% | 227978 | 225471 | -1.10% |
| B | 95% 读 / 5% 更新 | +3.03% | 330340 | 330817 | +0.14% |
| C | 只读 | +0.30% | 369357 | 369088 | -0.07% |
| D | 95% 读 / 5% 插入，latest | -0.49% | 538419 | 541310 | +0.54% |
| E | 95% 扫描 / 5% 插入 | +2.23% | 40734 | 40684 | -0.12% |
| F | 50% 读 / 50% 读改写 | -1.99% | 196799 | 196289 | -0.26% |

完整长测六项 RunTime 合计：关闭 4160.907 秒，开启 4168.906 秒；同样操作量下汇总吞吐变化 -0.19%。它受较慢的 E 权重影响较大，不代表生产流量权重。A 的更新 P99 从 378 µs 增至 397 µs（+5.03%）；F 的更新 P99 从 157 µs 增至 166 µs（+5.73%）。其余 P99 与总耗时见完整报告。

完整长测没有重现短测的 B/E 小幅收益。两批的预热时长也不同，不能仅把结果变化归因于正式负载时长。完整长测只有一轮，不能估计轮间波动或统计显著性；组内 A–F 继承前序数据库状态，因此后段是整套流程效果。YCSB 使用其 OVERALL 吞吐，**没有额外追加 `compactall`**。

## JNI jar 校验

原 Maven `rocksdbjni-6.26.1-linux64.jar` 含 Java 11 class major 55，与容器 Java 8 不兼容。重新从目标提交构建，Maven 构建及测试通过，Java 8 class major=52，ELF=AArch64。更新了 Maven 中带/不带 `linux64` classifier 的 jar；本轮使用隔离的 YCSB 分发目录，未使用原目录中历史 target 产物。

- 构建：`PORTABLE=1 DEBUG_LEVEL=0 make rocksdbjava -j12 DISABLE_WARNING_AS_ERROR=1 DISABLE_JEMALLOC=1`。并行数按容器约束选择。
- Jar SHA256：`8e25505e855ae3a07d9bdfd2256d8984c9df9bf115212635285783c9e7e95e63`。
- Native SHA256：`3fea1133ce8e972e1f5d53125e623d15203c72d00943d0feebf05fd2c128bbe0`。
- `java.library.path` 指向空目录，强制从 jar 提取 native；探针中实际映射的 native 哈希与 jar 内文件一致。YCSB 冒烟测试确认类来源为新 jar，1000 次插入全部成功。
- native 的 Git SHA 字符串为空；源码来源由目标提交的干净源码树、构建命令、jar/native 哈希与运行时加载记录确认，不能仅凭 jar 文件名断言版本。
- 预检关闭组 ZERO_RANGE 调用为 0；开启组 WAL/flush/compaction 调用为 2/2/1，无 fallback 或失败。正式阶段逐项核对 OPTIONS 开关。两批 jar 在执行前后均核对一致。

详情见 [校验清单](ycsb-120s-20261125/jar-verification.json) 与该目录的原始日志归档。

## 数据、日志与复现

每批目录保留 `summary.md`、逐项 `results.json/results.csv`、环境、audit 与实际执行脚本。YCSB 还保留原始 shell/ini 和用于 Maven 构建的 binding 源码、POM、workload 快照。`ycsb-120s-20261125/ycsb-source` 包含当时本地修改后的输入，不能用未经核对的上游默认 binding 代替。

原始日志按批压缩，归档内部路径相对原测试目录；不含 jar、native 二进制、数据库、Maven 仓库备份或构建目录：

| 批次 | 日志归档 | 文件数 |
|---|---|---:|
| db_bench | [raw-logs.tar.gz](db-bench-20261124/raw-logs.tar.gz) | 778 |
| YCSB 120 秒 | [raw-logs.tar.gz](ycsb-120s-20261125/raw-logs.tar.gz) | 180 |
| YCSB 1 亿操作 | [raw-logs.tar.gz](ycsb-100m-20261126/raw-logs.tar.gz) | 61 |

各批 `raw-logs-manifest.json` 记录归档成员大小和 SHA256，[artifact-manifest.json](artifact-manifest.json) 记录公开文件指纹。解压示例：

```bash
mkdir extracted
tar -xzf ycsb-100m-20261126/raw-logs.tar.gz -C extracted
```

执行脚本保留实际使用的绝对路径、jar 哈希和资源检查。复现时先准备对应源码/工具/数据，修改脚本中的路径常量以及 shell 配置路径，使用新的结果和数据目录；完整长测不能直接覆盖原目录重跑。所有 RocksDB/YCSB 测试继续经 `sudo docker exec lb_rocksdb_16u ...` 运行，确认 CPU、内存及无 swap 配额。保留 archive 作为证据，不需要为了阅读报告重新运行基准。
