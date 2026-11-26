# YCSB 完整 1 亿操作对照

> 版本范围：测试 `0a4721027`，与第一阶段 `cd7a6a37f` 的 tree 完全相同；未测试后续 `e19156074`。整体结论及实验差异见 [汇总](../README.md)。

目标提交 0a47210279850a86ee97e8fc1dae846d4a110db3，同一个已验证 JNI jar；A1 关闭 ZERO_RANGE，B 开启。
16 CPU / 32 GiB / 无 swap；每组从同一 1 亿记录备份独立复制；先完成 1 亿次只读预热（8 线程），再依次完成 A-F 各 1 亿次操作（16 线程）。
无执行时间上限，实际操作数逐项核对为 100000000；不额外执行 compactall，不清空宿主机缓存。使用原 common.sh 的 warm_up/run_workload，JVM heap 4–8 GiB。
这是关闭/开启各一轮完整测试，不是三轮中位数，也不是父提交/目标提交的整个版本对比。WAL 开启、sync=false、concurrent memtable write=true。

| workload | 关闭 ops/s | 开启 ops/s | 开启/关闭 | 关闭/开启 RunTime 秒 |
|---|---:|---:|---:|---:|
| workloada | 227977.9 | 225471.5 | -1.10% | 438.639/443.515 |
| workloadb | 330340.4 | 330816.9 | +0.14% | 302.718/302.282 |
| workloadc | 369356.7 | 369088.1 | -0.07% | 270.741/270.938 |
| workloadd | 538418.9 | 541310.1 | +0.54% | 185.729/184.737 |
| workloade | 40734.1 | 40683.8 | -0.12% | 2454.947/2457.981 |
| workloadf | 196798.9 | 196289.0 | -0.26% | 508.133/509.453 |

## P99 延迟

| workload | 操作 | 关闭 P99 us | 开启 P99 us | 变化 |
|---|---|---:|---:|---:|
| workloada | READ | 327 | 339 | +3.67% |
| workloada | UPDATE | 378 | 397 | +5.03% |
| workloadb | READ | 191 | 188 | -1.57% |
| workloadb | UPDATE | 211 | 207 | -1.90% |
| workloadc | READ | 152 | 154 | +1.32% |
| workloadd | INSERT | 79 | 77 | -2.53% |
| workloadd | READ | 196 | 194 | -1.02% |
| workloade | INSERT | 67 | 66 | -1.49% |
| workloade | SCAN-LATENCY-PER-RECORD | 96 | 97 | +1.04% |
| workloade | SCAN | 1306 | 1308 | +0.15% |
| workloadf | READ-MODIFY-WRITE | 476 | 486 | +2.10% |
| workloadf | READ | 397 | 392 | -1.26% |
| workloadf | UPDATE | 157 | 166 | +5.73% |

每项数据已检查错误返回值、异常、实际操作数、OPTIONS 开关；jar SHA256 在执行前后复核。
开关两组分别独立恢复备份，但组内 A-F 按原脚本继承前面的数据库状态；一轮结果不能描述轮间波动，后段差异也包含前序负载引起的状态差异。
YCSB 吞吐采用其 OVERALL 指标；process_seconds 另含启动与脚本开销。完整原始指标见 results.json/results.csv，命令及日志在 A1、B 目录，环境见 environment.json。
计时期间没有其他编译、扫描、分析或并行基准负载；低频进度读取在宿主机 CPU16（测试 cpuset 之外）进行。共享宿主机其他活动没有强制隔离。

## 结论与资源复核

完整长测未观察到整体吞吐提升。A -1.10%，B +0.14%，C -0.07%，D +0.54%，E -0.12%，F -0.26%。上轮 120 秒测试中 B/E 的 +3.03%/+2.23% 未在本轮重现。

六项各完成同样 1 亿操作，按六项 RunTime 求和：关闭 4160.907 秒，开启 4168.906 秒；汇总吞吐变化 -0.19%。此汇总按原脚本各负载同等操作量计算，受较慢的 E 影响较大，不代表生产负载权重。

A 的 UPDATE P99 从 378 us 增至 397 us（+5.03%）；F 的 UPDATE P99 从 157 us 增至 166 us（+5.73%）。本轮各一遍，未做统计显著性检验，不能据此给出跨环境的确定结论。

与上轮相比，正式负载取消了 120 秒限制，预热也从 120 秒变为完整 1 亿次只读操作，因此两轮结果差异不能仅归因于正式负载时长。

测试时间：2026-11-26T09:19:53+0800 至 2026-11-26T11:59:41+0800。12 项正式负载与 2 次预热实际操作数均为 100000000，合计 14 亿次逻辑操作；未重新执行 load，使用已有 1 亿记录备份库。无操作错误，内存 failcnt 与 OOM kill 均保持 0；swap/total_swap 均为 0。容器仍保持 16 CPU、32 GiB、无 swap。

jar SHA256 在运行前后核对一致：`8e25505e855ae3a07d9bdfd2256d8984c9df9bf115212635285783c9e7e95e63`。Java 8/aarch64 与运行时 native 校验沿用相同 artifact 的前次验证，详情见 jar-verification.json。原始流程和配置见 original-scripts，实际调用见 full-common.sh、A1/B 下的 *.command.json，控制器见 run_full.py。
