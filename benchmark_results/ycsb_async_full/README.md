# YCSB 完整1亿操作流程

沿用前次五种候选：native_p0、native_p1、async_w1_lazy1、async_w2_lazy1、async_w2_lazy0。每种只跑一遍。16 CPU容器lb_rocksdb_16u、16前台线程；WAL开启，WriteOptions.sync=false，保持原YCSB binding语义。

每种配置开始时恢复备份的独立物理副本，预热C=1亿次读取（8线程），再依次执行A、B、C、D、E、F，各1亿次逻辑操作。A→F共用同一数据库，保留数据演化。maxexecutiontime=0，无时间上限。每次结束核对Throughput×RunTime推算的逻辑操作数恰为100000000，全部非OK状态必须为0。

使用原common.sh的warm_up和run_workload函数，原workload文件和dbsnappyrun.ini；新增的配置仅控制async/worker/lazy/pipeline，并固定concurrent=true。环境与前次相同：Java11、-Xms2g -Xmx4g，LD_LIBRARY_PATH清空、java.library.path为空目录，避免加载旧native库；每个JVM核对实际JNI映射SHA256和生效参数。

原始备份位于/data/lb/workload_backup_data，工作目录/data/lb/ycsb-async-full-85ab/active。完整恢复使用物理复制，不使用SST硬链接。复制后逐文件fsync并posix_fadvise(DONTNEED)，仅清理测试副本缓存，不执行影响整台主机的drop_caches。原备份及现有workload_data不变。

30个正式样本（5配置×6负载）加5次完整预热，共35亿次逻辑操作。结果与此前限时样本分开存储，不混入中位数；本次每组只有一个样本，不计算统计显著性或宣称全局最优。

jar_verification.json记录测试前jar指纹；每轮.native.json记录实际映射，.rocksdb.log记录实际选项。results.jsonl保存逐轮指标，summary.csv/report.md由tools/summarize_ycsb_async.py --out=benchmark_results/ycsb_async_full生成。

## 仓库归档

原始日志、逐轮命令和运行时映射已移至同目录的 `raw_logs.tar.gz`，可用 `sha256sum -c raw_logs.sha256` 校验后解压。`raw_files.json` 保存解压文件清单及各文件校验值。文中宿主机未完成数据和旧 Jar 备份仅保留在采集机，未计入本报告，也未发布。
