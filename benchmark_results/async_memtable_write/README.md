# 测试配置

正式数据通过 docker exec 在 lb_rocksdb_16u 中采集。CPU quota=1600000/100000，cpuset=0-15，mems=0，内存上限32GiB。注意0-15在本机是8个物理核上的16个SMT线程。

前台32线程；key=16B，value=100B；预装100万条；SkipList、无压缩、无block cache、buffered I/O；WAL开启，sync使用db_bench的默认fdatasync路径；64MiB write buffer，max_write_buffer_number=4，max_background_jobs=4。固定CPU范围，操作随机种子42；每轮恢复同一份预装且已compact的SST数据库，SST使用硬链接（不可变），其他文件独立复制。

只读readrandom；50%读写readrandomwriterandom（每线程50次Get后50次Put）；纯写overwrite（直接Put，没有前置Get）。读写混合吞吐为读写总ops/s。

sync、enable_pipelined_write、allow_concurrent_memtable_write均取true/false；async启用时worker取1/2/4/8/16，lazy_advance_sequence取true/false。关闭concurrent只支持worker=1，其他组合被源代码拒绝，未偷偷调整参数。原生对照显式async=false、worker=0，lazy保持true以满足参数校验。

168个有效组合，每组合10秒，重复3轮。每轮全部配置打乱顺序，串行运行，避免相互争用。无单独预热，数据集较小，结果代表本配置下的短时内存命中负载，不代表大库长期稳态。throughput包括异步排空，P99是API返回延迟；lazy=true的write返回等待WAL，lazy=false等待序列发布。DB关闭时间不计入主吞吐。CPU秒包含进程启动与关闭，仅作辅助指标。

manifest.json保存源码commit、二进制SHA256、CPU配额/affinity和全部参数；每轮command.json及log保留命令与原始输出，results.jsonl保留逐轮值。summary.csv为三轮中位数，附min/max及CV；P99列为三轮P99的中位数。report.md和results.html由 tools/summarize_async.py生成。

源码已经revert zero range，然后cherry-pick目标分支最近6个commit，产品源码与97fc7d395一致。Release构建为-O2、NDEBUG，构建日志保留了assert专用变量的unused-variable告警；未改产品源码。复现时可使用 make db_bench DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1。测试使用独立Debug构建（-O1，保留assert/SyncPoint）。async_commit_test 151项、async_commit_executor_test 979项通过。

宿主机48核的未完成采集已归档到同级async_memtable_write_host_48cores_incomplete，不纳入此报告。

## 仓库归档

原始日志、逐轮命令和运行时映射已移至同目录的 `raw_logs.tar.gz`，可用 `sha256sum -c raw_logs.sha256` 校验后解压。`raw_files.json` 保存解压文件清单及各文件校验值。文中宿主机未完成数据和旧 Jar 备份仅保留在采集机，未计入本报告，也未发布。
