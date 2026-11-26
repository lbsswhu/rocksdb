# YCSB ZERO_RANGE A1/B 对照

> 版本范围：测试 `0a4721027`，与第一阶段 `cd7a6a37f` 的 tree 完全相同；未测试后续 `e19156074`。整体结论及实验差异见 [汇总](../README.md)。

目标提交 0a47210279850a86ee97e8fc1dae846d4a110db3，同一个经过验证的 JNI jar；A1 关闭 ZERO_RANGE，B 开启。
16 CPU / 32 GiB / 无 swap；原始 1 亿记录备份，A→F 顺序；每组先只读预热 120 秒，每项正式负载 120 秒，三轮。
每轮每组重新复制同一备份，组内保持原脚本的连续 A→F；未清空宿主机缓存。JVM heap 4–8 GiB。
统计为三轮中位数；不是父提交与目标提交的整个版本对比。原 binding 为默认 WAL 开启、sync=false。

| workload | A1 ops/s | B ops/s | B/A1 | 三轮配对变化 |
|---|---:|---:|---:|---|
| workloada | 267648.4 | 263970.2 | -1.37% | +1.54% / -2.20% / -1.37% |
| workloadb | 322580.1 | 332365.5 | +3.03% | -0.05% / +4.30% / +3.37% |
| workloadc | 365474.0 | 366569.4 | +0.30% | +0.30% / +2.14% / -0.45% |
| workloadd | 511448.3 | 508959.9 | -0.49% | -1.17% / -0.09% / -0.55% |
| workloade | 36466.6 | 37279.8 | +2.23% | +3.66% / +0.61% / +2.23% |
| workloadf | 202753.1 | 198711.8 | -1.99% | -0.57% / -3.50% / -2.43% |

全部运行检查非 OK 返回值和异常；原始日志含 READ/UPDATE/INSERT/SCAN/READ-MODIFY-WRITE 的延迟分位数。
YCSB Throughput 使用其 OVERALL 指标，包含该版本客户端线程内的 datastore init/cleanup；未额外强制全库 compactall。
jar-verification.json 记录 jar/native SHA256、Java class 版本、来源提交；preflight 日志验证 native 映射哈希与 jar 内 native 一致及真实 ZERO_RANGE 调用。
共享宿主机与操作系统缓存未完全隔离；每种 workload 顺序继承前面 workload 的状态，因此后段是整套脚本效果，不是独立 workload 的因果隔离。
原脚本/配置快照见 original-scripts；限时适配见 timed-common.sh；运行命令见 *.command.json。

## 结论

本轮中位数：B +3.03%、E +2.23%；A -1.37%、D -0.49%、F -1.99%，只读 C +0.30%。未观察到普遍提升。E 三轮均为正（+0.61% 至 +3.66%），B 有一轮接近零；A/F 的回退也应纳入取舍。三轮样本未用于统计显著性检验，配对变化范围不是置信区间。

## 操作延迟 P99

以下为三轮各自 P99 的中位数，非合并直方图；负的变化表示延迟降低。

| workload | 操作 | 关闭 P99 us | 开启 P99 us | 变化 |
|---|---|---:|---:|---:|
| workloada | READ | 218 | 232 | +6.42% |
| workloada | UPDATE | 254 | 269 | +5.91% |
| workloadb | READ | 185 | 185 | +0.00% |
| workloadb | UPDATE | 206 | 205 | -0.49% |
| workloadc | READ | 154 | 155 | +0.65% |
| workloadd | INSERT | 77 | 76 | -1.30% |
| workloadd | READ | 199 | 200 | +0.50% |
| workloade | INSERT | 68 | 68 | +0.00% |
| workloade | SCAN-LATENCY-PER-RECORD | 109 | 108 | -0.92% |
| workloade | SCAN | 1644 | 1613 | -1.89% |
| workloadf | READ-MODIFY-WRITE | 434 | 466 | +7.37% |
| workloadf | READ | 352 | 375 | +6.53% |
| workloadf | UPDATE | 160 | 163 | +1.88% |

## 校验与复现

测试时间：2026-11-25T17:35:04+0800 至 2026-11-25T19:03:50+0800。36 项正式负载及 6 次预热均成功；未发生非 OK 操作返回值或异常，未发生 OOM、内存 failcnt 增长或 swap 使用。

计时期间不运行编译、搜索、磁盘扫描、性能采样器或并行基准。低频进度读取绑定在宿主机 CPU16，位于测试 cpuset 0-15 之外；全部日志解析在单项计时结束后进行。未强制隔离宿主机其他进程。

原有 Maven artifact 使用 Java 11 class version 55，容器 Java 8 无法编译；现已备份至 previous-maven-artifact，并将 Maven 中带/不带 linux64 classifier 的 jar 及 SHA1 替换为重新构建的 Java 8 jar。原 YCSB 目录的历史 target 产物未用于本轮。运行使用隔离分发目录 runtime，强制空 java.library.path 以从 jar 提取 native，避免加载系统残留 JNI。

实际 jar SHA256：`8e25505e855ae3a07d9bdfd2256d8984c9df9bf115212635285783c9e7e95e63`；native SHA256：`3fea1133ce8e972e1f5d53125e623d15203c72d00943d0feebf05fd2c128bbe0`。ELF 为 AArch64，Java class major=52；运行时 native 映射哈希与 jar 内内容一致。native 的 git SHA 字符串为空，来源由目标提交的干净源码、构建命令及这些哈希记录确认。

预检中关闭组 ZERO_RANGE=0；开启组 WAL/flush/compaction 调用分别为 2/2/1，fallback/failure 均为 0。正式阶段使用同一 jar，逐项核对 OPTIONS 的开关与 allow_concurrent_memtable_write=true。

只读预热用原 warm_up 的 8 线程；正式负载用原 run_workload 的 16 线程。recordcount/operationcount 保留 100000000，并增加 maxexecutiontime=120。沿用原脚本函数和配置，跳过 setup_envir 中对固定数据目录的删除及宿主机 drop_caches；数据恢复由控制器在独立目录完成。此轮没有重新执行 1 亿记录 load，使用已有 117 GiB 备份库。

`run_matrix.py` 是本次控制器，`timed-common.sh` 是限时适配，`original-scripts` 保留原脚本和配置；`ycsb-src` 保存用于 Maven 构建的源码副本。复跑需使用新的结果/数据目录并相应调整控制器中的 R、D，避免覆盖本次结果。

完整逐项指标见 results.json/results.csv；jar 校验见 jar-verification.json、probe-A1.log、probe-B.log、ycsb-jar-smoke.log；Maven 构建与测试见 maven-correct-jar.log；每项原始命令与输出在 r*-A1/r*-B 下。
