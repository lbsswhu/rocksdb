# YCSB async memtable 对比配置

## 原始脚本与复用方式

本次source `/home/lb/YCSB/test_scripts/inc/common.sh`，直接调用其中的 `warm_up` 和 `run_workload`；`run.sh` 指定的A–F、16前台线程、1亿recordcount和1亿operationcount原样保留，使用原始workload文件及 `dbsnappyrun.ini`。原文件快照见source_snapshot。

复用 `/data/lb/workload_backup_data` 的117GiB备份。每轮复制所有可变文件、硬链接不可变SST，恢复到独立的 `/data/lb/ycsb-async-85ab/active`，未操作现有workload_data。没有重新load1亿条数据，没有执行原setup_envir的全局drop_caches，也不做原脚本A→F的累积数据库演化；每个配置/负载都从同一备份开始。

在原脚本函数外增加bin/ycsb包装器，仅设置Java环境和 `maxexecutiontime`。每轮先调用原warm_up（8线程、只读C、限时10秒），然后run_workload（16线程、最多60秒）；如1亿操作先完成，则以实际完成时间为准。预热与测量是独立JVM，预热主要针对数据缓存。全部30个配置/负载组合随机排序、每轮3次，共90个正式样本；预检和预热排除在性能统计外。运行串行执行。

## 配置矩阵

|名字|async|workers|lazy_advance_sequence|pipeline|concurrent|
|---|---|---|---|---|---|
|native_p0|false|0|true（未启用async）|false|true|
|native_p1|false|0|true（未启用async）|true|true|
|async_w1_lazy1|true|1|true|false|true|
|async_w2_lazy1|true|2|true|false|true|
|async_w2_lazy0|true|2|false|false|true|

所有其他RocksDB配置原样保留，包括384MiB write buffer、4 memtables、逐层NoCompression/Snappy/Snappy/ZSTD、原始后台任务和限速设置。配置文件的use_fsync=true是同步时选择fsync，并不启用逐次写入同步；原binding的Put没有显式WriteOptions，因此本实验WriteOptions.sync=false、WAL开启。未将这个设置错误描述为每次WAL同步。

YCSB默认10字段×100B，约1KiB记录，readallfields=true。A=50%读/50%update，B=95/5，C=只读，D=95%读/5%insert/latest，E=95%scan/5%insert/scan长度1–100，F=50%读/50%read-modify-write。保留原binding实现，UPDATE自身包含先Get再Put的行为，不能与db_bench直接Put吞吐等同。

## 容器与Java

全部正式样本在lb_rocksdb_16u中执行，CPU quota=1600000/100000=16、cpuset=0-15、mems=0，内存限制32GiB；本机0-15为8物理核/16 SMT线程。Java11 BiSheng，统一-Xms2g -Xmx4g；测量保留每次新JVM的JIT/GC影响。YCSB的RunTime在所有ClientThread cleanup并join后结束，因此包括RocksDB关闭/异步排空开销。

## Jar核对

以PORTABLE=1 DEBUG_LEVEL=0、DISABLE_WARNING_AS_ERROR=1、DISABLE_JEMALLOC=1构建rocksdbjava。新jar同时更新Maven中的无classifier和linux64两个文件名及sha1；原文件保存在original_maven_jars。YCSB当前pom实际引用linux64 classifier，仅替换无classifier文件不够。随后在YCSB目录执行mvn -pl site.ycsb:rocksdb-binding -am clean package，打包依赖也已核对。

本机.bashrc包含旧test_scripts/library和另一个rocksdb/java/target。本次每个进程LD_LIBRARY_PATH清空，java.library.path指向独立空目录，强制从新jar提取JNI。预检启用类加载日志证明jar来源；每个正式/预热进程都检查实际/proc/PID/maps中的native库并核对SHA256，保存为.native.json。jar_verification.json同时核对构建jar、两个Maven jar、YCSB打包依赖，以及jar内和实际映射的native库。

## 输出

results.jsonl为逐轮YCSB指标；summary.csv为中位数/min/max/CV与P99；report.md为汇总。每轮保留原始YCSB输出、原common.sh生成的日志、native映射证据和实际RocksDB LOG（含生效参数）。manifest.json记录源码、原始文件/构建文件校验值和容器限制。与历史runlog性能不混用。

这是按用户确认的限时筛选，覆盖五种候选模式，不是上轮db_bench全参数矩阵，也不宣称已找出全部worker数量中的全局最优配置。

YCSB启动器会在容器内root重编译打包binding，归档Built-By等元数据变化的审计见binding_archive_audit.json；从保存的测试前源码独立编译得到的三个class与实际binding jar逐字节一致，JNI jar与native指纹全程未变。

## 仓库归档

原始日志、逐轮命令和运行时映射已移至同目录的 `raw_logs.tar.gz`，可用 `sha256sum -c raw_logs.sha256` 校验后解压。`raw_files.json` 保存解压文件清单及各文件校验值。文中宿主机未完成数据和旧 Jar 备份仅保留在采集机，未计入本报告，也未发布。
