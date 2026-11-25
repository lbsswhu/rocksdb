# async_memtable_write 基准测试归档

本目录保存 db_bench 和 YCSB 两类测试，包括两轮 YCSB 实验。所有正式性能样本均在 `lb_rocksdb_16u` 容器中采集：CPU quota=16、cpuset=0-15（本机8物理核/16 SMT线程）、mems=0、内存上限32GiB。

| 实验 | 报告 | 样本 | WAL sync |
|---|---|---|---|
| db_bench 参数矩阵 | [中文分析](async_memtable_write/analysis_zh.md) / [CSV](async_memtable_write/summary.csv) | 168个有效组合，每组10秒×3，共504轮 | true / false |
| YCSB A–F 限时筛选 | [中文分析](ycsb_async/analysis_zh.md) / [CSV](ycsb_async/summary.csv) | 5种候选×6负载，每组60秒×3，共90轮 | false |
| YCSB A–F 完整流程 | [中文分析](ycsb_async_full/analysis_zh.md) / [CSV](ycsb_async_full/summary.csv) | 每配置完整预热1亿读，再A→F各1亿次；共30项正式、5次预热 | false |

db_bench 的 lazy=true 纯写相对本轮最佳原生配置提高73.7%（sync=false）/38.5%（sync=true）。完整 YCSB A–F 未观察到吞吐优势：最佳 async 候选相对最佳原生为 A -6.1%、B -2.1%、C -0.1%、D -4.1%、E -0.6%、F -7.7%。这些结论限于各报告中的硬件、负载与参数；YCSB 完整流程每组只有一个样本。

YCSB 的 `use_fsync=true` 选择同步时使用的系统调用，不会启用逐次写入同步。原 binding 的 Put 未设置 `WriteOptions.sync=true`。UPDATE 包含 Get 后再 Put，默认约1KiB记录，与 db_bench 的100B直接 Put 模型不同。

## 源码与 Jar 身份

实测工作树提交为 `f01cbdddc`，产品文件树与公开分支提交 `97fc7d395c2e7b363ac5c258d80c27e375ed0de2` 完全一致，两者 tree 均为 `120db9bf6bb7e0553184c2be461c9df1bbf759c0`。详见 [source_provenance.json](source_provenance.json)。公开分支已不包含 zero range 预分配；本提交仅新增测试工具和数据。

YCSB 新 JNI jar 同时更新 Maven 无 classifier 和 linux64 classifier 两个文件名，并重新打包 YCSB。测试隔离旧 native 库路径，核对 jar 内 `.so` 与每个 JVM 实际映射的 native SHA256；证据见各 YCSB 目录的 `jar_verification.json`、`binding_archive_audit.json` 和原始日志。

## 原始证据

各实验的报告、CSV、逐轮 `results.jsonl`、配置、源文件快照、运行环境及校验结果可直接浏览。原始输出、RocksDB LOG、逐轮命令和 JNI 映射记录按原相对路径存放在各自的 `raw_logs.tar.gz` 中，避免把上百MB文本逐行展开到 Git 历史。

在相应实验目录执行：

```sh
sha256sum -c raw_logs.sha256
tar -xzf raw_logs.tar.gz
```

`raw_files.json` 保存每个解压文件的字节数和 SHA256。日志中的绝对路径、PID、容器ID、编译提交哈希保留采集原貌；这些路径与本机布局相关。

未提交初期宿主机48核上的未完成采集、旧 Maven Jar 备份、编译产物或数据库副本。它们未计入正式结论。

## 重新汇总

在仓库根目录执行以下命令即可从逐轮 JSONL 重新生成 CSV 与表格，无需解压日志或重跑性能测试：

```sh
python3 tools/summarize_async.py
python3 tools/summarize_ycsb_async.py
python3 tools/summarize_ycsb_async.py --out=benchmark_results/ycsb_async_full
```

db_bench 的 `results.html` 是本地交互表格；GitHub 源码浏览页不运行其中的 JavaScript，下载后用浏览器打开即可。

## 重新采集

脚本按采集机的 Linux cgroup v1、AArch64、Java11、Docker挂载目录编写。需准备同样的 YCSB checkout、备份数据和容器；`run_ycsb_async.py` 中 `/home/lb/YCSB`、`/data/lb/workload_backup_data` 等路径需对应实际环境。复用仓库内的 `source_snapshot` 可核对工作负载、配置及 binding 改动。使用新的输出目录，保留本次归档。

先构建与分支一致的 Release db_bench / JNI：

```sh
PORTABLE=1 DEBUG_LEVEL=0 make db_bench -j64 DISABLE_WARNING_AS_ERROR=1
JAVA_HOME=/usr/lib/jvm/java-11-openjdk PORTABLE=1 DEBUG_LEVEL=0 \
  make rocksdbjava -j64 DISABLE_WARNING_AS_ERROR=1 DISABLE_JEMALLOC=1
```

db_bench 需要先建立 `--root` 目录下的 `seed` 数据库；[原始准备命令输出](async_memtable_write/raw_files.json)中的 `prepare.log` 以及[参数说明](async_memtable_write/README.md)记录数据规模。参考命令（独立数据目录）：

```sh
./db_bench --benchmarks=fillseq,flush,compact \
  --db=/data/lb/async-memtable-bench-85ab/seed \
  --num=1000000 --threads=1 --key_size=16 --value_size=100 \
  --compression_type=none --async_memtable_write=false \
  --lazy_advance_sequence=true --enable_pipelined_write=false --sync=false
```

在容器内、仓库根目录运行：

```sh
python3 tools/run_async_bench.py --cpus=0-15 \
  --out=benchmark_results/reproduced_db_bench
python3 tools/summarize_async.py --out=benchmark_results/reproduced_db_bench
```

YCSB 先将构建的新 jar 安装至 Maven 的两个实际文件名及 SHA1，并在 YCSB 目录执行 `mvn -pl site.ycsb:rocksdb-binding -am clean package`；确认打包依赖的 SHA256 一致。由于默认 shell 存在旧 native 路径，采集驱动会清空 `LD_LIBRARY_PATH` 并从新 jar 提取 JNI。进入同样挂载的16 CPU容器后，在 RocksDB 仓库根目录运行：

```sh
python3 tools/run_ycsb_async.py --probe --out=benchmark_results/reproduced_probe
python3 tools/run_ycsb_async.py --out=benchmark_results/reproduced_ycsb
python3 tools/run_ycsb_async.py --full-suite --repeats=1 --seconds=0 \
  --out=benchmark_results/reproduced_ycsb_full
```

三种采集应串行运行；YCSB 驱动会删除并重建其专用 `active` 数据库目录，完整流程在本机约需7小时。构建前的旧 Jar 备份仅保留在采集机，仓库保存指纹而不分发这些二进制。
