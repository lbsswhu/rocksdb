# YCSB 完整1亿操作流程结果

## 结果

完整流程未观察到async吞吐优势。每个负载均比较本轮候选中的最佳async与两种pipeline中较快的原生配置：A下降6.1%、B下降2.1%、D下降4.1%、F下降7.7%；C/E接近持平。对于这套A–F配置，本轮观察值支持继续使用原生路径。

2026-11-25 09:06启动，约15:58完成，总耗时约6小时52分钟。五种配置各跑一遍：独立物理副本→C预热1亿次→A/B/C/D/E/F各1亿次，共30项正式测试和5次完整预热，合计35亿次YCSB逻辑操作。全程maxexecutiontime=0，所有项目均核对为100000000次，未用时间截断。

运行于lb_rocksdb_16u，CPU quota=16、cpuset=0-15（8物理核/16 SMT线程）、mems=0、内存32GiB；16前台线程，预热8线程。WriteOptions.sync=false、WAL开启，保留原dbsnappyrun.ini的use_fsync=true及原binding写入方式。每种配置内A→F使用同一数据库，保留累积数据演化。详细设置见[README](README.md)。

## 吞吐

每个配置只测一遍。ops/s为完成1亿次操作的单轮平均值，不是三轮中位数；本轮不提供统计显著性结论。

|负载|最佳原生|原生ops/s|最佳async|async ops/s|变化|
|---|---|---|---|---|---|
|A|native_p1|235,449|async_w1_lazy1|221,001|-6.1%|
|B|native_p0|331,689|async_w2_lazy0|324,826|-2.1%|
|C|native_p1|367,126|async_w1_lazy1|366,746|-0.1%|
|D|native_p1|542,997|async_w2_lazy0|520,980|-4.1%|
|E|native_p1|40,937|async_w2_lazy0|40,689|-0.6%|
|F|native_p1|204,813|async_w1_lazy1|188,999|-7.7%|

async_w1_lazy1=1 worker/lazy=true；async_w2_lazy1=2 workers/lazy=true；async_w2_lazy0=2 workers/lazy=false。三个async候选的pipeline=false；所有配置concurrent=true。

A/F中1 worker、lazy=true为最佳async候选；B/D/E中2 workers、lazy=false较好，但仍未超过本轮最佳原生。C没有写入，不据此挑选memtable worker数量。2 workers且lazy=true在A/F的吞吐分别仅200,463/165,985 ops/s，比最佳原生低14.9%/19.0%，增加worker没有带来单调提升。

## P99

与上表相同，取最佳吞吐的原生和async候选对比；单位µs，每格为原生→async。每格均是对应单轮的P99。

|负载|读P99|UPDATE P99|SCAN P99|
|---|---|---|---|
|A|326→335|374→372|—|
|B|187→189|205→227|—|
|C|154→154|—|—|
|D|191→200|—|—|
|E|—|—|1318→1323|
|F|387→391|148→145|—|

尾延迟没有普遍改善。F的UPDATE P99略低，但其读P99略高，整体吞吐下降7.7%。不能以某一个延迟分位数的小幅变化代替吞吐和全部读写延迟的评估。

## 原生pipeline控制

|负载|pipeline=false ops/s|pipeline=true ops/s|变化|
|---|---|---|---|
|A|231,743|235,449|+1.6%|
|B|331,689|327,418|-1.3%|
|C|366,408|367,126|+0.2%|
|D|536,360|542,997|+1.2%|
|E|40,520|40,937|+1.0%|
|F|199,999|204,813|+2.4%|

## 与先前60秒筛选的关系

全量结果没有改变此前“这套YCSB A–F未见明确async收益”的判断。原生的绝对吞吐部分高于60秒测试；本轮同时改用了完整预热、每配置内A→F累积演化和物理数据库副本，因此不把绝对QPS变化单独归因于运行时长。

YCSB UPDATE会先Get旧记录，再Put序列化后的完整记录；F还包含外层Read，记录约1KiB，初始备份117GiB，超过容器内存。这些与此前db_bench的100B直接Put只写模型不同。db_bench的lazy=true纯写收益仍是另一个已测场景的结果，不能代替这里的A–F数据。本轮没有测初始load纯插入吞吐，也没有扩大至4/8/16 workers或逐次WAL同步。

## 校验与产物

35个JVM的实际native映射均核对SHA256，且CPU允许列表为0-15、cgroup属于测试容器。每项逻辑操作数均通过OVERALL Throughput×RunTime及对应操作计数两种方式核对为1亿，所有非OK返回为0。实际async/worker/lazy/pipeline/concurrent参数与配置一致。见[validation.json](validation.json)。

构建jar、两个Maven文件名、YCSB打包依赖的SHA256一致；jar内native与实际映射的native一致，见[jar_verification.json](jar_verification.json)。清空LD_LIBRARY_PATH、使用空java.library.path，避免加载旧test_scripts/library。YCSB启动器每次Maven重编译打包使binding归档时间戳变化；从测试前源码独立编译的三个class与实际binding jar逐字节一致，见[binding_archive_audit.json](binding_archive_audit.json)。

产品源码与目标async分支一致，zero range已revert。原备份CURRENT/MANIFEST/CF_NAMES内容校验值不变。每种配置开始时恢复独立物理副本；仅对副本文件做fsync/fadvise清理缓存，没有操作原workload_data，也没有执行宿主机全局drop_caches。

[完整30组表格](report.md)、[CSV](summary.csv)、[逐轮数据](results.jsonl)保留全部结果。原始YCSB输出、原common.sh生成的日志、RocksDB LOG、native映射、运行命令及源文件快照均在本目录中，与60秒测试分开保存。
