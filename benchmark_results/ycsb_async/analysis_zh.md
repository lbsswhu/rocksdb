# YCSB async_memtable_write 结果分析

## 结论

在本次A–F限时筛选的五个候选中，没有测到稳定的async吞吐收益。A、D、F出现可重复的吞吐回退；B、C、E与原生接近，差异落在本轮样本波动范围内。建议这套配置继续使用原生路径。

已完成30个配置/负载组合×3次，共90个正式样本。16 CPU容器内执行、16前台线程，复用既有117GiB备份，recordcount=1亿、默认10字段×100B。每轮恢复独立副本、用原脚本预热函数限时预热10秒，再测最多60秒。范围和配置差异见[README](README.md)，实际测量时间为60.025–60.411秒，包含ClientThread清理和数据库关闭。

## 吞吐对比

下表取三次中位数。原生从pipeline关闭/开启中取较快配置；async从1 worker+lazy开、2 workers+lazy开、2 workers+lazy关中选最佳。全部保持concurrent=true和原始配置的WriteOptions.sync=false；use_fsync=true不等于逐次WAL同步。

|负载|最佳原生|原生ops/s|最佳async|async ops/s|变化|async三次范围|原生三次范围|
|---|---|---|---|---|---|---|---|
|A|native_p1|221,810|async_w1_lazy1|210,600|-5.1%|208,313–213,427|221,058–225,518|
|B|native_p1|268,179|async_w2_lazy0|266,851|-0.5%|254,635–268,331|267,038–272,595|
|C|native_p1|276,581|async_w2_lazy0|277,211|+0.2%|272,016–277,467|268,976–277,742|
|D|native_p0|478,601|async_w2_lazy0|455,252|-4.9%|454,831–457,741|478,507–487,008|
|E|native_p1|35,281|async_w2_lazy1|35,408|+0.4%|34,979–35,708|35,032–35,311|
|F|native_p1|214,476|async_w1_lazy1|195,571|-8.8%|191,262–196,482|210,824–214,907|

A、D、F的最佳async样本最大值仍低于对应最佳原生样本最小值，三次范围不重叠。B/C/E的范围重叠，不把C的+0.2%或E的+0.4%解释为可靠收益。这些范围是观察到的min/max，不是统计置信区间。

## P99延迟

仍比较上表选中的最佳吞吐配置，单位µs；每格为原生→async，P99取三次P99的中位数。

|负载|读P99|Update P99|Scan P99|
|---|---|---|---|
|A|320→321|351→347|—|
|B|276→270|295→313|—|
|C|270→272|—|—|
|D|222→225|—|—|
|E|—|—|1726→1753|
|F|312→311|100→110|—|

没有与吞吐同时出现的普遍尾延迟改善。B中关闭lazy的2 workers候选读P99略低，但Update P99更高。F中最佳async的Update P99为110µs，高于原生100µs。

## 参数选择

原生pipeline=true在A、B、C、E、F取得最高中位数，D为false；C/E等小幅差异不构成全局最优证明。两种pipeline下的原生及全部async原始结果见[完整表](report.md)与[CSV](summary.csv)。

如果只比较本次async候选：A/F为1 worker、lazy=true；B/C/D为2 workers、lazy=false；E为2 workers、lazy=true。增加到2 workers并始终开lazy没有带来更高吞吐：A仅187,221、F仅165,391 ops/s，分别比最佳原生低15.6%/22.9%。本轮未扩展至4/8/16 workers或逐次WAL同步，不宣称找出全部参数组合的全局最优值。

## 与db_bench结果的关系

原YCSB binding的UPDATE会先Get当前记录，再Put序列化后的完整记录；F还包含read-modify-write的外层Read。负载含读取、字段反序列化/序列化，记录约1KiB，数据集也大于容器内存；这些都与此前100B、100万key的直接Put模型不同。不能将db_bench纯写的收益直接代入这套A–F结果。本次没有额外profile，未把差异归因于某个单独锁、拷贝或调度事件。数据加载阶段没有重测。

## Jar与运行时校验

从当前已revert zero range、包含六个async提交的源码构建新JNI jar。新jar、Maven无classifier文件、Maven linux64 classifier文件、YCSB打包依赖的SHA256全部一致：

`fb3d369fda8cd406246c1948a93680e9fc3a029af05ed0aa7c08606ce2fe975d`

当前pom实际使用linux64 classifier；仅替换无classifier文件不足以保证运行版本。原shell还含旧native库路径。本次清空LD_LIBRARY_PATH并把java.library.path指向空目录，确保从新jar提取JNI；预检保存类加载来源，每个正式/预热JVM保存实际native映射及SHA256，共180个进程校验通过。jar内native、构建native与实际映射的native一致。详见[jar_verification.json](jar_verification.json)。

YCSB源代码启动器每轮都会调用Maven。容器内root重编译打包使binding归档的Built-By等元数据不同于初次宿主机构建，binding整个归档指纹随之变化。使用测试前保存的Java源码独立编译，三个binding class与实际归档逐字节一致；RocksDB JNI jar和native指纹在全程未变。详见[binding_archive_audit.json](binding_archive_audit.json)。

## 完整性

90个正式样本、90次预热均通过运行检查，正式READ/UPDATE/INSERT/SCAN返回计数与各自Operations一致，非OK返回为0。所有180个JVM的CPU允许列表为0-15、cgroup属于测试容器，生效async/worker/lazy/pipeline/concurrent参数与配置一致。备份CURRENT、MANIFEST、CF_NAMES校验值未变；产品源码与目标分支保持一致。

[validation.json](validation.json)记录检查结果。逐轮YCSB日志、原common.sh生成的日志、RocksDB LOG、生效配置、native映射、命令和源文件快照均保留。所有历史runlog和JNI预检数据均未混入性能样本。
