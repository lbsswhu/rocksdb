# RocksDB ZERO_RANGE 按需预分配设计

> 新增：[2026-11-24 至 2026-11-26 性能测试汇总](docs/benchmarks/zero-range-preallocation/README.md)，包含 16 CPU/32 GiB/无 swap 的 db_bench、YCSB 120 秒三轮和完整 1 亿操作对照，以及原始数据、日志和 JNI 校验。这批测试对应 `0a4721027`（与第一阶段 `cd7a6a37f` 的 tree 相同），未覆盖后续 `e19156074` 优化；与第 15 节实验独立。

## 1. 状态、需求与基线

本文确定配置、接口、算法、状态转换和错误处理，作为编码与验收依据。第一阶段已提交为 `cd7a6a37f`；第二阶段按第 15 节优化旧 WAL 收尾和 Append 快路径，工作区实现、正确性验证及实际性能结果在该节和对应报告中记录。

- 源码：`/home/lb/rocksdb`，`v6.26.1`。
- 原始性能基线：`4d57a393a8f1563c0dff1ff468756c23d89e3601`；优化前功能基线：`cd7a6a37f`。
- 目标：降低 WAL、Flush SST、Compaction/Subcompaction SST 的写入时延，提高只写吞吐。
- 手段：按需分段执行 `fallocate(fd, FALLOC_FL_ZERO_RANGE, offset, len)`，不附加 `FALLOC_FL_KEEP_SIZE`；完成写入后裁剪到有效长度。
- 新功能默认开启，同时提供显式关闭开关，用于回滚及同一二进制的性能对照。
- 性能验收：使用 `db_bench` 的只写负载，覆盖 WAL 同步/异步、Flush、Compaction 和 SST Direct I/O。

本设计保留按需扩容，不采用工作区 `preallocate-design.md` 中的“创建时一次性分配 1.5 倍、后续不再申请空间”策略。两份文档不能混合作为实现要求；本任务以本文为准。

“inode 更新”在此具体指预分配可能提前扩大 `i_size`，不是 inode 编号改变。空间分配成功不等于有效数据已经写入，更不等于数据已经持久化。

### 1.1 五项需求与验收对应

| 需求 | 设计约定 | 验收证据 |
|---|---|---|
| 直接使用 ZERO_RANGE，降低 WAL/SST 写入时延 | 第 6 节只传 `FALLOC_FL_ZERO_RANGE`，不附加 KEEP_SIZE | 第 13 节 syscall 证据及只写 A/B；不能仅凭 flag 替换认定时延下降 |
| 覆盖 WAL、MemTable Flush、Compaction 新 SST | 第 7—8 节接入三条关键路径；验收范围按第 2 节执行 | 三类文件分别命中新模式，Compaction 必须实际生成输出 SST |
| 写完后 truncate 到实际位置 | SST 完成、WAL 轮转和正常关闭时裁到逻辑末尾 L | 最终长度正确、裁尾失败上报、截断后按协议同步 |
| 默认开启 | DBOptions 和 db_bench 默认 true，低层按文件用途选择模式 | 不传新 flag 的运行实际命中 ZERO_RANGE，同时验证显式关闭 |
| 使用 db_bench 只写模型验证性能提升 | 第 13 节执行 release A0/A1/B，计入分配、裁尾及收尾成本 | 原始日志、吞吐/延迟、后台工作和完整收尾结果；改善须超过运行噪声 |

以上是设计约定与待完成的验收项，目前均未作为新功能实现或性能验证结果交付。第 13 节的命令模板和目标值不等于已经验证性能提升。

## 2. 范围和兼容边界

### 2.1 必须覆盖

| 文件来源 | 当前创建入口 | 新模式收尾边界 |
|---|---|---|
| DB 打开及轮转产生的 WAL | `DBImpl::CreateWAL()` | 轮转时封口，DB 正常关闭时处理最后一个 WAL |
| MemTable Flush SST，包括 atomic flush 的各文件 | `FlushJob::WriteLevel0Table()` → `BuildTable()` | Builder 完成后、成功安装前 |
| Compaction/Subcompaction SST | `CompactionJob::OpenCompactionOutputFile()` | 每个输出文件完成时 |
| WAL 恢复生成的 SST | `WriteLevel0TableForRecovery()` → `BuildTable()` | 与普通 BuildTable 相同 |
| Repair 生成的 SST | Repair 的 WAL 转换 → `BuildTable()` | 与普通 BuildTable 相同 |
| 外部新建 SST | `SstFileWriter::Open()` | `SstFileWriter::Finish()` 成功返回前 |

Ingestion、硬链接、复制已有 SST、trivial move 不重新清零已有文件。MANIFEST、CURRENT、OPTIONS、诊断 LOG、blob、临时导出文件不自动套用新模式；它们继续原有行为。

### 2.2 默认开启的具体含义

Linux、本地 POSIX、普通 buffered 写和 SST Direct I/O、新建 WAL/SST 默认尝试 ZERO_RANGE。兼容处理如下：

| 情况 | 行为 |
|---|---|
| `enable_zero_range_preallocation=false` | 恢复原有分配入口和粒度；不为原本没有预分配的 Flush/外部 SST 额外开启分配 |
| `allow_fallocate=false` | 尊重显式禁止，不调用 fallocate；新开关不覆盖它 |
| 编译环境无 ZERO_RANGE 或非 Linux | 使用原有实现，记录未启用原因 |
| 文件系统明确不支持该 mode | 当前文件降级到原有分配方式，记录原因和计数 |
| mmap 写入、自定义/加密/远端 FS 未实现该能力 | 不向下传递 ZERO_RANGE 请求，使用原有实现 |
| `recycle_log_file_num>0` | 首版整个 WAL recycling 写路径保持原策略，包括该配置下的新建 WAL；SST 仍可启用 |
| 空间不足、真实 I/O 错误、裁尾失败 | 上报错误；不能伪装成“不支持”后静默继续 |

首版支持同进程 `GetUpdatesSince()`。**启用新模式的主库与跨进程活跃 WAL 追尾（Secondary 或旧版在线 WAL 消费者）的组合，首版不支持。** 跨进程读者拿不到下文的完整记录可读边界，单靠文件大小无法区分正在写入的记录与预分配零尾部。使用这类部署必须在主库设置 `enable_zero_range_preallocation=false`，不能只在 Secondary 关闭。主库无法可靠检测其他进程，必须在选项说明与发布说明中明确此限制。

只读打开已经正确封口的 SST/数据库不受此限制。改进后的恢复读取必须在新开关关闭时仍能识别以前留下的零尾部。首版不承诺旧版程序可以直接恢复未经正常收尾的新模式 WAL；降级前应正常关闭并验证 WAL 已裁尾。

### 2.3 本次验收范围

本次功能及性能验收固定使用 `recycle_log_file_num=0`，不包含 WAL recycling，也不包含 Secondary 等跨进程活跃 WAL 追尾。两者不作为本次交付的通过条件；第 2.2 节保留其兼容行为和部署限制，不承诺此次实现已经适配这些场景。

同进程 `GetUpdatesSince()`、新版本崩溃恢复、正常关闭和备份有效前缀仍属于验收范围。第 12—14 节的正确性矩阵、db_bench 运行和交付条件均遵循此范围。

## 3. 当前代码分析

### 3.1 按需分配的实际调用链

```text
log::Writer::AddRecord() / TableBuilder 写出数据
  → WritableFileWriter::Append()
      → FSWritableFile::PrepareWrite(offset, len)
          → 跨过预分配粒度时 Allocate(offset, len)
              → PosixWritableFile::Allocate()
                  → fallocate(fd, keep_size ? KEEP_SIZE : 0, offset, len)
```

- [WritableFileWriter::Append](file/writable_file_writer.cc#L39) 在处理追加缓冲前调用 PrepareWrite；因此按需分配仍在追加路径中。
- [FSWritableFile::PrepareWrite](include/rocksdb/file_system.h#L982) 按粒度向上取整，当前忽略 Allocate 错误，失败后也推进 `last_preallocated_block_`。
- [FSWritableFile 构造](include/rocksdb/file_system.h#L803) 的默认分配粒度为 0；仅替换 Allocate 的 flag，不能使原本未设置粒度的路径自动获得预分配。
- [PosixWritableFile::Allocate](env/io_posix.cc#L1381) 当前使用 `KEEP_SIZE` 或模式 `0`，没有 ZERO_RANGE 分支。

### 3.2 WAL

- [CreateWAL](db/db_impl/db_impl_open.cc#L1533) 创建/复用文件后设置分配粒度，再创建 WritableFileWriter 和 log::Writer。
- [GetWalPreallocateBlockSize](db/db_impl/db_impl_write.cc#L1962) 以 `write_buffer_size + floor(write_buffer_size/10)` 为基础，并受非零 WAL/DB/WriteBufferManager 容量限制。打开时和轮转时的调用参数沿用当前逻辑，本次不顺带修改多 CF 选取策略。
- [POSIX OptimizeForLogWrite](env/fs_posix.cc#L963) 禁用 WAL mmap/direct 写，强制 `fallocate_with_keep_size=true`。旁边明确记录：关闭 keep_size 可能更快，但会破坏 `TransactionLogIteratorStallAtLastRecord`。
- [SwitchMemtable](db/db_impl/db_impl_write.cc#L1757) 在创建新 WAL 后，对旧 WAL 执行 WriteBuffer，再发布新日志；旧 Writer 不一定当场关闭。
- [SyncWAL / MarkLogsSynced](db/db_impl/db_impl.cc#L1335) 可以并发同步旧日志，并将 Writer 延迟放入 `logs_to_free_`。因此不能把“轮转”“Writer 析构”“WAL 删除”当作同一时间点。

### 3.3 SST

- [FlushJob::WriteLevel0Table](db/flush_job.cc#L787) 调用 BuildTable；[BuildTable](db/builder.cc#L55) 当前没有显式设置预分配粒度。
- [OpenCompactionOutputFile](db/compaction/compaction_job.cc#L1970) 已设置 OutputFilePreallocationSize。
- [OutputFilePreallocationSize](db/compaction/compaction.cc#L510) 使用输入文件总大小，在相应条件下取单输出文件上限，再加约 10%，最大 1 GiB。
- [SstFileWriter::Open](table/sst_file_writer.cc#L222) 当前没有显式设置预分配粒度。
- BuildTable 与 Compaction 收尾当前是 Builder Finish → Writer Sync → Writer Close。

### 3.4 现有裁尾不能直接承担新语义

- [PosixWritableFile::Close](env/io_posix.cc#L1259) 只有在记录到预分配时才尝试 ftruncate，而且忽略其返回值；随后可能 punch hole。
- [WritableFileWriter::Close](file/writable_file_writer.cc#L193) 对 Direct I/O 先 Flush、Truncate、Fsync，再关闭；buffered I/O 没有同样的上层强制裁尾。
- Direct I/O Flush 可能保留缓冲中的尾页以便后续重写；若先裁尾、Close 又重新 Flush，文件会再次被补齐。
- [ReadFooterFromFile](table/format.cc#L285) 根据传入文件长度定位 footer，必须保证有效大小与最终文件大小一致。
- [Reader::ReadPhysicalRecord](db/log_reader.cc#L347) 遇到零记录会清空当前块并继续推进；预读的零字节可能在未来被 Writer 覆写，旧缓存和读游标不能继续视为有效。
- [POSIX NewSequentialFile](env/fs_posix.cc#L192) 使用 fdopen 创建 FILE*，[PosixSequentialFile::Read](env/io_posix.cc#L230) 调用 fread_unlocked。stdio 自身仍有预读缓冲；即使上层 Read 请求不超过完整记录边界，也可能在底层提前缓存边界外的零尾部。因此有界 FSSequentialFile 包装必须配合禁用该读取路径的 stdio 缓冲。

## 4. 确定的实现方案

本次采用 **ZERO_RANGE 按需分段预分配、显式封口裁尾、WAL 完整记录可读边界**。本章固定实现路线，后续章节细化同一方案，不再留给编码阶段选择其他预分配策略。

| 环节 | 确定行为 | 具体设计 |
|---|---|---|
| 配置 | `enable_zero_range_preallocation=true`，提供关闭开关；低层仅对目标 WAL/SST 选择新模式 | 第 5 节 |
| 分配入口 | 在 WritableFileWriter 新增返回 IOStatus 的 PrepareAppend，使用独立成功分配水位 P | 第 6 节 |
| 分配方式 | 写入超过 P 时调用 `fallocate(fd, FALLOC_FL_ZERO_RANGE, P, E-P)`；不附加 KEEP_SIZE；文件继续增长时重复按需分配 | 第 6 节 |
| SST 覆盖 | BuildTable、OpenCompactionOutputFile、SstFileWriter::Open 三个入口统一接入 | 第 7 节 |
| SST 完成 | Builder Finish → Flush → Truncate(L) → Sync/Fsync → Close；成功后才能安装或返回创建成功 | 第 7 节 |
| WAL 轮转及关闭 | 旧 WAL 停止追加后写出缓冲、裁尾；轮转不增加强制同步，正常关闭执行最终同步 | 第 8 节 |
| WAL 在线读取 | Writer 仅在完整记录写出后发布 R；同进程读者在预读层之前限制读取到 R | 第 9.1 节 |
| 崩溃恢复 | 保存完整有效记录末尾 V，验证预分配零尾部，保留原有 checksum 与损坏恢复语义 | 第 9.2 节 |
| 错误与降级 | 不支持的环境走第 2.2 节兼容路径；分配、写出、裁尾、同步的真实错误返回调用者 | 第 6—10 节 |

实现依赖当前已有的 Allocate、Truncate、Sync/Fsync、文件创建入口和 WAL Writer/Reader；新增部分是明确的软件状态与调用顺序，不要求修改内核或引入后台文件池。平台和部署支持范围按第 2.2 节执行。

功能验收检查系统调用、数据内容、最终长度、恢复和在线读取；性能验收使用第 13 节的 db_bench 对照。实现路线已经确定，性能提升幅度由该对照给出，不将尚未测量的百分比写成实现保证。

## 5. 配置、能力与状态

### 5.1 配置贯通

新增不可动态修改的 DB 选项：

```cpp
// 在支持的平台上，对新建 WAL/SST 尝试 ZERO_RANGE 按需预分配。
bool enable_zero_range_preallocation = true;
```

必须同时接入：

- `include/rocksdb/options.h` 的 DBOptions 声明及兼容说明。
- `options/db_options.h/.cc` 的 ImmutableDBOptions、构造、选项注册、Dump。
- `options/options_helper.cc` 的 BuildDBOptions 反向转换，OPTIONS 序列化及加载测试。
- `tools/db_bench_tool.cc` 的同名 bool flag，默认值取 DBOptions，并赋给实际打开 DB 的 Options。
- 外部 SstFileWriter 使用传入 Options 的同一开关。

低层 EnvOptions/FileOptions 增加 `PreallocationMode { kLegacy, kZeroRange }`，**低层默认值为 kLegacy**。只有 WAL/SST 的专门创建入口在 DB 开关允许且能力满足时设置 kZeroRange，禁止在 AssignEnvOptions 中把 DB 开关无条件复制为所有文件的低层模式。

`fallocate_with_keep_size` 仍控制 legacy 模式；kZeroRange 分支直接使用 ZERO_RANGE，不与 KEEP_SIZE 做 OR。WAL OptimizeForLogWrite 中现有 keep_size 赋值不应覆盖专门选中的新模式。

FileOptions 另增内部读取选项 `bool disable_stdio_buffering = false`。所有使用 WalReadBoundary 的 POSIX WAL 顺序读取在打开文件前将其设为 true；该选项不作为 DBOptions 或 db_bench 用户开关。默认/转换构造初始化为 false，FileOptions 拷贝构造与赋值保留实际值，经过读取选项优化及包装层时不得丢失。POSIX 必须在 fdopen 成功后、第一次读取前用它控制 setvbuf，具体协议见第 9.1 节。

### 5.2 能力协商与实际模式

增加以下非纯虚接口：

```cpp
// FileSystem：默认 false，只有已审计的实现返回 true。
virtual bool SupportsZeroRangePreallocation() const;

// FSWritableFile：默认 kLegacy；POSIX 返回当前实际模式，降级后改变。
virtual PreallocationMode GetPreallocationMode() const;
```

POSIX 在编译支持时声明能力；运行时是否支持由真实调用确认。包装层只有确认选项、分配、长度和关闭语义，以及 WAL 有界读取和禁用底层预读的约定都正确转发后才能转发能力。加密 FS 等未适配实现保持 false，避免下层被清零而上层仍按旧语义工作。

FileSystemWrapper、FSWritableFileWrapper、IO tracing、Composite/Legacy 桥接及测试 FS 必须审计。没有能力声明时，创建入口在调用底层前将模式恢复 legacy；不能依赖底层“可能忽略新字段”。新增虚接口意味着需要完整重编译依赖方，不能混用旧 ABI 的插件。

### 5.3 长度和状态

| 状态 | 含义 | 禁止混用 |
|---|---|---|
| `L` | WritableFileWriter 成功接受的逻辑字节末尾 | 可能还有数据留在用户态缓冲 |
| `W` | 底层成功写出的字节末尾 | Direct I/O 可能包含补齐字节 |
| `P` | 成功分配的连续区间末尾 | 不代表有效数据 |
| `F` | 文件系统报告的 st_size | 新模式下可能大于 L |
| `R` | 成功写到 OS 的完整 WAL 逻辑记录末尾 | 不代表 fsync 成功 |
| `V` | 恢复扫描最后一条完整有效 WAL 记录末尾 | 不能用扫描到的物理 EOF 代替 |

Writer 保存 `reserved_end_`、`sealed_`、`needs_final_truncate_`；POSIX 文件对象保存实际模式及独立的裁尾需求。按需成功计数与“是否可能扩大过 EOF”分别维护：即使分配失败或发生降级，也不能因此漏掉关闭清理。

核心不变量：

1. ZERO_RANGE 只操作本文件尚未写入的尾部，绝不能清零 `[0,W)` 的有效内容或已写出的 Direct I/O 尾页。
2. 只有 Allocate 成功才推进 P；只有完整 WAL 记录成功写出才发布 R。
3. 成功封口后 `F == L`，SST 长度、footer、FileMetaData 和 checksum 一致。
4. `sealed_` 后不再 Append 或重新写出补齐尾页。
5. Allocate、truncate、sync 的关键错误均不能被转换成成功。

## 6. 按需分配算法及错误处理

### 6.1 避免改动旧 PrepareWrite 的虚接口

当前 PrepareWrite 返回 void。本文不将其直接改成返回 Status，也不让新路径继续经过会吞错的旧算法。

在 WritableFileWriter 中新增返回 IOStatus 的内部 `PrepareAppend(offset, len)`：

- legacy 路径调用原有 `writable_file_->PrepareWrite()`，保持原行为。
- 新路径从 `GetPreallocationStatus()` 获取配置粒度 B，使用 Writer 自己的 `reserved_end_`，直接调用返回 IOStatus 的 `Allocate()`；不再调用旧 PrepareWrite，也不推进旧的块计数。
- 新路径的选择在 Writer 创建时固定；底层运行时降级不切回另一套计数算法，仍用同一成功水位管理本文件后续请求。
- Append 在修改 checksum、缓冲内容和 L 之前检查分配状态。失败时原样返回，不把本次追加标记为成功。

伪代码：

```text
PrepareAppend(offset, len):
    如果不是新路径：调用原 PrepareWrite；返回 OK
    如果 len == 0：返回 OK
    如果 offset <= P 且 len <= P-offset：返回 OK
    首次扩容读取并验证 B/对齐，缓存固定粒度；B == 0 返回 InvalidArgument
    检查 offset + len、向上对齐及 off_t 上限，禁止溢出
    E = AlignUp(offset + len, B)
    如果是 Direct I/O：确保 E 同时覆盖最终补齐写出的末尾
    如果 E <= P：返回 OK
    要求 P >= 当前已写出的 W；首次请求 P=W=0
    在可能扩大 EOF 前置 needs_final_truncate=true
    s = Allocate(P, E-P)
    如果失败：返回 s，不推进 P
    P = E
    返回 OK
```

B 向上对齐到文件要求的 I/O 对齐值，Direct I/O 分配边界也保持对齐。第一次分配必须发生在本文件第一次实际写出之前。不得在已经写过数据的文件上以 P=0 重新启动新算法。

### 6.2 粒度

| 来源 | B 的来源 |
|---|---|
| WAL | 沿用 GetWalPreallocateBlockSize 及现有容量限制，只补充检查和对齐 |
| Compaction | 沿用 OutputFilePreallocationSize，只补充检查和对齐 |
| BuildTable：Flush/恢复/Repair | 使用该 CF 的 `write_buffer_size + floor(write_buffer_size/10)`，限制最大 1 GiB，再做 I/O 对齐 |
| 外部 SstFileWriter | 使用传入 CF 配置的同一 SST 粒度算法 |

SST 粒度下限为所需 I/O 对齐值。新模式在文件创建时设置粒度，首次 Append 扩容前验证并缓存对齐后的粒度：配置基数为 0、乘加或对齐溢出、结果超出 off_t 时返回 InvalidArgument；此时不得修改逻辑大小、checksum 或缓冲内容，候选文件由错误收尾关闭。B 是增长粒度，超过当前 P 时继续扩容。首版不另加粒度调优配置。

### 6.3 POSIX Allocate

新模式执行 `fallocate(fd, FALLOC_FL_ZERO_RANGE, offset, len)`。不修改顺序写 fd 的当前位置，不以预分配后的 EOF 作为下一次写入位置。

- EINTR：重试同一区间，该区间必须始终满足未写入约束。
- EOPNOTSUPP/ENOSYS：记录降级，当前文件改用原有 Allocate 模式；后续不反复尝试 ZERO_RANGE。
- EINVAL、ENOSPC、EDQUOT、EIO、超限：返回真实错误。EINVAL 不作为能力降级依据，避免猜测它来自不支持的 mode 还是参数/实现错误。
- 一次失败的系统调用可能已经改变部分文件状态；在调用前就标记裁尾需求。支持性降级后也继续保留该标记。
- 错误收尾只清理临时文件/未完成输出，不得按包含未写缓冲的 L 扩大文件、制造看似有效的零数据。

低层 Close 的裁尾条件改为“旧预分配状态或独立裁尾标记”。新模式下 ftruncate 失败必须返回错误；它不再是仅影响空间回收的优化。析构仍只能作资源释放兜底，不能作为唯一可报告错误的收尾路径。

## 7. SST 接入与最终收尾

### 7.1 创建入口

在创建文件前生成明确的 WAL/SST FileOptions，能力检查通过才设置新模式。BuildTable 与外部 SstFileWriter 在第一次 Builder 写出前设置 B；Compaction 保留现有粒度设置。

BuildTable 的改动统一覆盖普通 Flush、恢复与 Repair，不在每条调用链重复实现分配。空输出或 Builder Abandon 不安装文件；range tombstone-only、超大 value、压缩率很高和 plain table 等路径也使用实际 Writer 长度。

### 7.2 Writer 统一封口

新增以下内部方法：

```cpp
IOStatus WritableFileWriter::Seal();
IOStatus WritableFileWriter::FinishAndClose(bool use_fsync);
```

`Seal()` 的语义：停止接受后续追加，成功 Flush 全部缓冲，按 L 调用底层 Truncate，标记最终长度变更需要同步。只有完成这些操作才进入成功 sealed 状态；失败状态禁止重试追加。legacy buffered 文件不额外截断，Direct I/O 保留必要截断。

状态转换固定为 `Open → Sealing → Sealed → Closed`；Seal/写出发生错误时进入 Failed，保存首个错误并仅允许资源释放。进入 Sealing 即禁止新的 Append。重复 Seal 在 Sealed 返回 OK，在 Failed 返回保存的错误。Sealed 后 Flush 返回 OK 且不写数据，Append 返回 InvalidArgument；Close 释放资源并保留此前错误，重复 Close 返回首次关闭结果。`sealed_`、失败状态及关闭结果必须共同落实这些行为。

`FinishAndClose()` 的语义：

```text
Builder::Finish() 已成功（包含 footer）
  → Seal：Flush → Truncate(L)
  → 若缺少覆盖封口状态的同级成功同步，按 use_fsync 执行真实 Fsync 或 Sync
  → 底层 Close
  → 成功后完成文件 checksum，并返回成功
```

必须处理以下细节：

- 新模式和 Direct I/O 必须由 truncate 后的真实成功同步覆盖，不能仅因 `pending_sync_==false` 或原 Direct I/O 跳过逻辑而漏掉；已有覆盖同一 sealed 状态和所需级别的成功同步时可复用（第 15.3 节）。
- Seal 后 Close/析构不能再次调用会重写尾页的 Flush；后续 Flush 按上述状态规则返回，底层 Close 也不能改变已确认的最终长度。
- 保留第一次错误，仍关闭 fd；错误文件不能进入 VersionEdit，也不能报告创建成功。
- Builder Finish 失败或 Flush 失败时，不执行以 L 为依据的成功裁尾/校验流程；走 Abandon/删除路径。
- 修改 BuildTable、FinishCompactionOutputFile、SstFileWriter::Finish 三处，使用同一收尾方法，避免只修一个入口。
- listener 通知必须携带真实操作状态；checksum 只覆盖逻辑内容，不覆盖预分配零尾部。

目录同步和 MANIFEST 安装继续现有协议。文件数据同步不能替代目录同步。fdatasync 也必须持久化读取所必需的文件大小变更。[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

## 8. WAL 写入、轮转、同步与关闭

### 8.1 写入与可读边界

log::Writer 增加 `pending_complete_end_` 和共享的 `WalReadBoundary`：

```cpp
struct WalReadBoundary {
  std::atomic<uint64_t> readable_end{0};
  std::atomic<bool> sealed{false};
};
```

- 每次 AddRecord 完整成功后，记录该逻辑记录末尾；不能在 header 或某个 fragment 写完时更新完整记录末尾。
- 自动 Flush 成功后，release 发布 readable_end=R。
- `manual_wal_flush=true` 时，AddRecord 只推进 pending；WriteBuffer 成功后才发布 R。
- 部分 AddRecord、Flush 或 I/O 失败不发布新 R；触发现有写错误处理。
- 审计 DB 写入、FlushWAL、LockWAL、轮转、关闭所有 Flush 入口；禁止绕过 log::Writer 的发布协议导致文件已经可读但边界永久滞后。
- 原有 WAL 大小统计、alive_log_files_、sequence 分配、sync WAL 元数据继续使用有效写入大小；不将 P 计入有效日志字节数。

R 是可读性水位，不是提交序列或持久化水位。事务 prepare/commit、WriteBatch 数量及 sequence 语义沿用现有实现。

### 8.2 轮转

在 SwitchMemtable 的 `creating_new_log` 分支：

1. 保留现有创建新 WAL 的步骤，但在旧 WAL 完成封口前不发布新 WAL。
2. 持有现有写队列的排他资格，确保包括 two_write_queues 的另一个写队列也不能向旧 WAL 追加。
3. 按既有 `mutex_ → log_write_mutex_` 顺序协调；若旧 WAL 正在 getting_synced，先在未持有 log_write_mutex_ 时通过 log_sync_cv_ 等待。等待后重新检查日志身份和状态。
4. 在禁止追加且没有并发 sync 操作改变同步状态的窗口内执行旧 Writer WriteBuffer → 发布最终 R → Seal/Truncate(L)，成功后标记共享边界 sealed。
5. 再发布 logfile_number_、logs_、alive_log_files_ 等新日志状态。旧 Writer 保留原来的延迟同步/释放生命周期。
6. Flush/截断失败：不发布新 WAL，关闭/清理候选文件，走现有 BGError/写错误路径；不得继续接收写入并掩盖失败。

本次不强制每次 WAL 轮转都 fsync。没有 sync 请求时保持原异步写持久性语义；封口后的长度变更必须进入下一次适用的 SyncWAL/同步写同步范围。在完成新同步前，不能把封口后状态标成已同步。

### 8.3 与 SyncWAL、Writer 回收配合

- 同步路径保持 getting_synced、log_sync_cv_、log_dir_synced_ 的原有职责；新增 Seal 参与同一同步协调，不能与“确认 WAL 已同步”交错后漏掉截断元数据。
- 对新模式，在一次同步开始前保存各 WAL 的 R 快照。同步成功后，MANIFEST 中确认的 WAL 大小只能使用该次已覆盖的完整记录水位，不能使用随后并发增长的 R、含未 Flush 缓冲的 L 或预分配大小 P。同步失败不发布新的已同步水位。
- MarkLogsSynced 释放旧 Writer 前应确认新模式文件已成功 sealed；该处不再隐式执行吞错裁尾。
- logs_to_free_、过期 WAL 清理和 archived WAL 的消费者不能假设析构时才裁尾。
- 不在锁顺序之外等待同步；不持有 log_write_mutex_ 再等待需要它的同步/释放线程。

### 8.4 DB 关闭

CloseHelper 在写线程退出后，对仍持有的所有 WAL Writer 执行显式关闭，包括最后一个未轮转 WAL：WriteBuffer → Seal/Truncate → 根据现有同步承诺完成必要同步 → Close。

关闭后必须得到真实有效长度；若要保证该长度在崩溃后也保留，则执行同步并将成本计入关闭时间。设计要求对新模式正常关闭确保至少一次覆盖封口后数据和最终长度的成功同步，使用 DB 的 use_fsync 选择；已有同级 sealed 成功同步则复用，封口前的同步不能复用。这会使异步写 DB 的正常关闭承担必要同步成本，必须单独测量。

错误通过 DB::Close() 返回；析构无法返回错误时记录日志。不能依赖“关闭时开关仍开启”，收尾由该 Writer 的实际历史状态决定。

## 9. WAL 在线读取与恢复

### 9.1 同进程 GetUpdatesSince：有界读取与禁用 stdio 预读

由 WalManager/DBImpl 按 WAL number 提供共享 WalReadBoundary；log::Writer、打开该 WAL 的迭代器共享同一个对象。

在 TransactionLogIteratorImpl::OpenLogReader 的文件打开路径增加有界 FSSequentialFile 包装，置于 readahead/SequentialFileReader 缓冲之下，并在打开底层 POSIX 文件时禁用 stdio 缓冲。调用顺序固定为：

```text
log::Reader
  → SequentialFileReader / RocksDB readahead
    → 有界 FSSequentialFile（按 R 限制 Read/Skip）
      → PosixSequentialFile（FILE* 已设为 _IONBF）
        → fread_unlocked（不缓存请求范围外的零尾部）
```

底层打开协议：

1. 在调用 NewSequentialFile 前设置 `FileOptions::disable_stdio_buffering=true`，不能先读取文件、缓存零尾部后再切换模式。
2. POSIX NewSequentialFile 在 fdopen 成功后，调用 `setvbuf(file, nullptr, _IONBF, 0)`；成功后才能交给上层。该设置保留普通 buffered I/O 和内核页缓存，不启用 Direct I/O。
3. setvbuf 返回非零时，fclose 释放文件并返回 IOStatus 错误；不能继续使用有缓冲的流。错误信息必须明确操作与文件名，不假定 setvbuf 一定设置 errno。
4. 包装层必须保留该选项及“不在有界层之下缓存超出请求范围字节”的语义；存在额外用户态预读缓存的实现只有落实该约定后才能声明能力。

完成上述设置后，有界包装执行：

```text
读取位置 pos，调用 Read(n)：
    limit = readable_end.load(acquire)
    只向底层请求 min(n, max(0, limit-pos)) 字节
    pos 到达 limit 时返回临时 EOF
```

**有界包装与禁用底层 stdio 预读必须同时实现。** 只限制传给 fread_unlocked 的 n 不足以保证安全：FILE* 可能一次缓存更大的范围，后续 Writer 覆写该范围时读者仍得到旧零数据。在两个条件都成立时，用户态读缓存才只包含已经发布的完整记录字节；不能先预读到 P 再靠 RestrictedRead 的 sequence 检查补救。边界增长后，通过现有 UnmarkEOF 协议继续读取，原来的半块读取逻辑仍然有效。

- Read/Skip、计数、readahead 与 EOF 位置必须一致；不能让一次大 Read 穿越 limit。
- R 发布前已经写入 OS 的字节暂时不可见是允许的；R 发布后不能永远被旧 EOF 或缓存挡住。
- 旧 WAL 封口后边界对象仍保留最终 R；Writer 销毁不能令已经打开的读者访问悬空指针。
- 注册表在删除/归档时安全释放自身引用，读者继续持有 shared_ptr；不要因清理注册表而把活跃读者改成无界读取。
- 对历史已关闭 WAL，没有注册项时使用静态恢复式读取；对本进程活跃新 WAL 缺失注册项是逻辑错误，不能退化为按 st_size 读取。
- DB 的 WAL iterator 成功返回边界、sequence 连续性和历史归档选择保持原语义。

边界不能只接在 TransactionLogIterator 上。[WalManager::GetSortedWalFiles](db/wal_manager.cc#L48) 会通过 ReadFirstRecord/ReadFirstLine 探测日志首记录，这些活跃 WAL 的读取也必须使用同一边界。R=0 时按“尚无完整记录”处理，不将临时空状态永久缓存；后续发布 R 后允许重新探测。

GetSortedWalsOfType/GetLiveWalFile 当前直接读取文件系统大小。对活跃新模式 WAL，日志 API 和备份/文件快照消费者应使用抓取时的 R 作为可复制的有效前缀长度；文件系统 F 仅用于物理空间观测。审计 `db/db_filesnapshot.cc` 等调用点：按原协议 Flush 必需的 WAL 缓冲、固定文件引用/禁止删除，并只复制该前缀，不能按 P 把预分配尾部作为有效日志导出。已封口文件使用最终长度。为此增加首记录探测、空 WAL、manual flush、备份后恢复的测试。

### 9.2 崩溃恢复：不能依赖正常 truncate

崩溃可能留下 `F=P>L`，也可能有一条写到一半的记录。恢复沿用 WAL checksum、fragment 拼接和 WALRecoveryMode，同时增加仅用于 WAL 的零尾部识别。

Reader 增加 WAL 专用读取选项，MANIFEST 等非 WAL reader 保持原行为。恢复步骤：

1. 成功读到完整有效记录时，显式保存逻辑记录末尾 V；不要在扫描零尾部后再调用当前 LastRecordEnd() 把物理读游标误当 V。
2. 在记录边界发现全零 header 时，将当前位置作为零尾部候选，流式检查后续到本次静态文件 EOF；内存用固定大小缓冲，不分配 P 大小内存。
3. 只有候选范围确认为全零，且没有正在组装的不完整逻辑记录，才能将其作为正常预分配尾部结束。
4. 若此前正在组装 fragmented record、发现 CRC/长度错误，必须保留该错误并按原 recovery mode 处理；零尾部不能抹掉已发生的损坏。损坏 payload 不能因为含零就被认定有效。
5. 若候选后有非零内容，不把它直接当正常尾部；继续既有零块/历史格式处理及错误检查，不能跳过未验证内容并宣称日志完好。
6. 全部待恢复 WAL 都适用，不能只处理编号最大的日志。上次轮转后崩溃、未持久化 truncate 也可能让旧 WAL 留有尾部。

恢复一般重放到 MemTable 并创建新 WAL，不重新清零/追加到旧文件；因此识别有效边界即可，无需为了恢复成功先修改历史 WAL。Repair、离线 WAL 导出/读取等入口应启用相同 WAL 规则，相关文件包括 db/log_reader、db_impl_open、repair、wal_manager、transaction_log_impl 和 ldb WAL 命令。

**此规则必须无条件保留在新版本读取端，不受写入开关控制。** 否则启用写入后关闭功能再重启，会失去恢复兼容性。

## 10. 可观测性与故障处理

按 DB 聚合以下统计，并按 WAL、Flush SST、Compaction SST、其他 SST 区分来源：

- ZERO_RANGE 调用次数、请求/成功字节数、总耗时和失败次数。
- 降级次数与原因；有效模式不能只打印“配置开启”。
- truncate 次数、耗时、失败次数；最终同步次数与耗时。
- 分配粒度、有效文件长度、预分配峰值空间和最终回收空间。

使用现有 Statistics 扩展 ticker/histogram，Writer 负责来源与请求统计，实际 mode 通过低层状态反馈；成功字节只在系统调用/实际 fallback 成功后计数。给 WAL Writer 贯通有效的统计对象，不能因当前构造传 nullptr 漏掉 WAL 数据。无 Statistics 时不引入每次写入的额外日志。

首次不支持时记录降级原因；高频写入不逐条打印。能力、开关、实际模式和测试选项写入测试记录，避免基准“开启”实际始终 fallback。

## 11. 代码改动清单与实施顺序

| 文件/区域 | 具体职责 |
|---|---|
| `include/rocksdb/options.h`、`options/db_options.*`、`options/options_helper.cc` | 默认 true、配置转换、序列化、日志及兼容说明 |
| `include/rocksdb/env.h`、`include/rocksdb/file_system.h` | 低层 mode、能力/实际模式查询；FileOptions 的 disable_stdio_buffering 及拷贝；保留旧 PrepareWrite 接口 |
| `env/io_posix.*`、`env/fs_posix.cc` | ZERO_RANGE、fallback、独立裁尾状态、WAL 读取在首次 I/O 前 setvbuf(_IONBF)、错误上报 |
| FS/Env wrappers、tracer、加密和测试 FS | 能力门控及读写选项转发审计，未适配者不向下传新 mode；有界层之下不得额外缓存未发布字节 |
| `file/writable_file_writer.*` | 返回状态的新按需算法、Seal、FinishAndClose、Direct I/O 尾页处理 |
| `db/db_impl/db_impl_open.cc`、`db_impl_write.cc`、`db_impl.cc` | WAL 创建、边界注册、轮转封口、同步与关闭 |
| `db/log_writer.*` | 完整记录末尾、Flush 后发布 R |
| `db/wal_manager.*`、`db/transaction_log_impl.*`、`db/db_filesnapshot.cc` | 边界生命周期、有界读取、首记录探测、备份有效长度、归档读取 |
| `db/log_reader.*`、恢复/Repair/离线读取调用点 | 显式保存 V，识别静态零尾部并保留损坏语义 |
| `db/builder.cc`、`db/compaction/compaction_job.cc`、`table/sst_file_writer.cc` | SST 粒度、选项选择、统一最终收尾 |
| `tools/db_bench_tool.cc`、测试与统计定义 | 开关、运行证据、性能/功能验证 |

按以下顺序实现，每步有对应验证，全部完成后才作为默认开启的功能交付：

1. 配置、能力门控、低层分配、Writer 成功水位和封口；先做独立文件测试。
2. 三个 SST 入口统一收尾，验证 buffered 与 Direct I/O。
3. WAL 完整记录边界、在线有界读取、底层 stdio 缓冲控制及恢复适配。
4. WAL 轮转、SyncWAL、关闭/回收的锁与错误处理。
5. 包装层、序列化、关闭开关、默认开启及降级回归。
6. 全部正确性通过后执行 db_bench A/B，并按实际结果给出是否提升的结论。

## 12. 正确性测试矩阵

以下矩阵按第 2.3 节执行：使用非 recycling WAL；跨进程追尾不列为本次必测项。

### 12.1 独立文件与 SST

- 边界：空文件、B-1/B/B+1、跨多个 B、超大单条写、整数溢出、首次写前分配。
- 对已写入前缀做 checksum，跨界分配后内容不变；确认没有重复清零此前的 Direct I/O 尾页。
- Flush、Compaction、多 Subcompaction、恢复/Repair、外部 SST 均出现真正 ZERO_RANGE 调用。
- buffered/Direct I/O、压缩/不压缩、range tombstone-only；最终 `stat.size == builder.FileSize == FileMetaData.size`。
- Finish 后用 sst_dump 验证 checksum、footer，再重新打开数据库核对数据。
- 注入 Allocate/部分分配/Flush/Truncate/Sync/Close 失败，输出不得安装；fd 仍释放；保留首次错误。
- 两次 Close、Seal 后析构、Direct I/O 已保留尾页：文件不能再次增长。
- 不支持 syscall/FS、allow_fallocate=false、mmap/custom FS 的降级及前缀保护。

### 12.2 WAL

- 自动/手动 Flush、sync=false/true、FlushWAL、LockWAL、多 CF、two_write_queues、并发 SyncWAL 与轮转。
- 轮转后旧 WAL 即使 Writer 还存活也必须已裁尾；正常 Close 处理最后一个及尚待释放的 Writer。
- 在分配后、header 后、middle fragment 后、Flush 前后、truncate 前后、sync 前后模拟进程崩溃。
- 每种 WALRecoveryMode 下验证完整记录、损坏中段、损坏尾段、零尾部以及旧 WAL 多文件重放。
- 用 `sync=true` 成功确认的记录必须恢复；异步写只按既有承诺验收，不要求恢复未落盘的数据。
- GetUpdatesSince 在记录后停住，再追加同一预分配范围内记录：一次 Next 应按现有测试语义继续读到，不能跳过或永远停在 P。
- POSIX 真实文件小读取回归：先 ZERO_RANGE 扩大文件，写入有效前缀，通过有界读者只读 16B；随后在偏移 16 写入非零内容并发布更大的 R，原读者必须读到新内容。读者不能重开，也不能通过手动清空缓存绕过问题；覆盖默认 libc 缓冲容易触发的非整块读取长度，并确认底层读取没有越过当次 R。
- GetUpdatesSince 集成回归：先写入小于 stdio 缓冲区的完整 WAL 记录并读至临时 EOF，再在同一预分配区间追加小记录，原迭代器继续读到正确内容；分别覆盖自动和 manual WAL flush、R 尚未发布和发布后、R 落在 WAL block 中间及随后跨 block。
- 验证 FileOptions 的 disable_stdio_buffering 在拷贝、读取优化和已适配包装层后仍为 true；注入 setvbuf 失败，打开必须报错并释放资源，不能回退为有缓冲读取。首记录探测使用同一无 stdio 缓冲的有界打开路径。
- 小记录、跨 WAL block 的大记录、预读缓存、跨分配边界、Writer 释放、WAL 归档及 manual_wal_flush 下均测试 R 的生命周期。
- 新模式写入后，以开关关闭重启；已裁尾旧文件与崩溃零尾部都能正确读取。
- 首记录探测在 R=0/R 增长后重试；活跃 WAL 文件列表大小正确；备份只复制有效前缀，备份恢复通过；MANIFEST WAL 大小不超过该次同步已确认水位。

现有可复用回归锚点：

- `db/db_log_iter_test.cc:153`：`DBTestXactLogIterator.TransactionLogIteratorStallAtLastRecord`，本方案必须通过，不修改断言来放宽语义。
- `env/env_test.cc:1687`：参数化 `EnvPosixTestWithParam.Preallocation`。
- `db/log_test.cc:375`、`:384`：参数化尾部截断恢复用例。
- FlushJob、CompactionJob、WritableFileWriter、external_sst_file、DBWAL、options 测试套件。

实现后先 `--gtest_list_tests` 确定当前构建的精确测试名，再执行聚焦过滤器，必须记录实际测试数量大于 0。本次已执行的套件、实际数量、聚焦回归和日志见第 15 节的验证记录。

## 13. db_bench 只写性能验证

### 13.1 对照与前置条件

ZERO_RANGE 的实现可使用 unwritten extents，不等于把所有零字节提前写到设备，也不保证消除后续 extent 转换。[fallocate(2)](https://man7.org/linux/man-pages/man2/fallocate.2.html) 本节测量实际设备上的吞吐和延迟变化，分配、裁尾、后台 drain 及最终同步成本均纳入统计，不通过减少持久性承诺获取更好的数字。

至少保留三组：

| 组 | 二进制/配置 | 目的 |
|---|---|---|
| A0 | 原始基线提交的 release 构建 | 原始性能参照 |
| A1 | 新实现，`enable_zero_range_preallocation=0` | 验证关闭开关回归及附加代码开销 |
| B | 同一新实现，`enable_zero_range_preallocation=1` | 用户要求的新模式 |

A1/B 是主对照，编译器、编译参数、设备、挂载参数、其余 DB 配置相同。B 默认开启，但测试中显式传 1，另用一次不传 flag 的短运行确认默认行为。主结论应包含 A0，不能只与本身已退化的 A1 比较。

所有正式运行固定 `recycle_log_file_num=0`，不启动 Secondary 或其他跨进程活跃 WAL 追尾消费者；本次不据此报告这些范围外场景的兼容性或性能结论。当前版本 db_bench 未暴露独立的 recycle_log_file_num flag，模板使用 DBOptions 的默认值 0，并以空 options_file 禁止从外部选项文件覆盖；每次运行仍须从实际 OPTIONS/选项输出核对该值，不能传入不存在的命令行 flag。

A0 使用单独保存的基线二进制和独立数据库，沿用相同 COMMON 参数，但删除基线不认识的 `--enable_zero_range_preallocation`；不能用新二进制的 mode=0 冒充原始版本。新增的计时/统计字段在 A0 不存在时标记为未采集。

实现完成后，先构建 release，例如：

```bash
DEBUG_LEVEL=0 make -j4 db_bench
DEBUG_LEVEL=0 make -j4 ldb sst_dump
./db_bench --help > /tmp/rocksdb-db-bench-help.txt 2>&1
```

`-j4` 按机器可用内存调整。确认构建带 gflags，并在 help 中确认新开关存在。当前实现已提供新 flag；原始 A0 不认识该参数，原始组应按第 13.1 节省略。

环境记录：commit、二进制 hash、编译命令、CPU/内存、内核版本、设备型号、实际文件系统和挂载参数、剩余空间、数据路径与 WAL 路径。当前工作区的 `stat -f` 只报告 ext 家族，不能由此认定基准设备就是预期 ext4 或证明支持 ZERO_RANGE；应在实际 DB 路径用 findmnt 和 syscall 验证。

### 13.2 负载矩阵

| 场景 | 负载 | 关键参数 | 用途 |
|---|---|---|---|
| 主场景 | fillrandom | WAL 开、sync=0、自动 compaction 开 | 前台只写 + WAL/Flush/Compaction 综合收益 |
| 持久化写 | fillrandom | WAL 开、sync=1、batch=1，1/4 线程 | 检验 sync 写延迟与 P99 |
| SST 场景 | fillrandom | disable_wal=1、sync=0 | 观察 Flush/Compaction 端贡献 |
| 顺序写 | fillseq | 1 线程，WAL 开 | 顺序增长对照，不能替代随机 compaction 场景 |
| 持续覆盖写 | overwrite | 从同内容的独立数据库副本开始 | 避免只测空库短暂填充 |
| Direct I/O | 重复主场景和 SST 场景 | use_direct_io_for_flush_and_compaction=1 | 检验补齐、裁尾与同步成本 |

每组至少 1 次预热和 5 次独立测量，交替 A/B 与 B/A 次序。每次用新数据库目录；overwrite 则从同一个正常关闭的基准数据库做独立副本，WAL 也位于各自独立目录中，不能让 A/B 顺序复用被上次运行修改的数据。

主要数据规模建议 1000 万次、16B key、1024B value、64MiB write_buffer、64MiB target SST、256MiB level base，实际可按空间调整。模板保留 12 个独立数据库，所需空间应按所有运行的累计结果及 Compaction 临时空间估算；空间不足时预先统一减小规模或将每轮校验后的数据库迁出基准盘，保留原始日志。必须产生多轮 Flush 和非 trivial Compaction；若后台计数为 0 或只做 trivial move，该运行不能验证 Compaction 输出优化。足够长的持续覆盖阶段用于观察稳定吞吐、写停顿和后台债务。

### 13.3 可执行 A/B 命令模板

以下 Bash 模板在**实现并构建新版本之后**执行。先把 BENCH_ROOT 设置到实际基准盘的现有目录；不使用生产数据库路径。每次 mktemp 创建独立输出，默认不删除结果。

```bash
set -euo pipefail
: "${BENCH_ROOT:?请设置 BENCH_ROOT 为基准盘上的已有测试目录}"
DB_BENCH=${DB_BENCH:-./db_bench}
TOTAL_WRITES=${TOTAL_WRITES:-10000000}
THREADS=${THREADS:-1}
SYNC_WRITES=${SYNC_WRITES:-0}
DISABLE_WAL=${DISABLE_WAL:-0}
DIRECT_SST=${DIRECT_SST:-0}
if (( THREADS <= 0 || THREADS > 4 || TOTAL_WRITES <= 0 || TOTAL_WRITES % THREADS != 0 )); then
  echo 'THREADS 必须在 1..4，TOTAL_WRITES 必须为正且能被 THREADS 整除' >&2
  exit 1
fi
if (( DISABLE_WAL != 0 && SYNC_WRITES != 0 )); then
  echo '本矩阵不组合 disable_wal=1 与 sync=1' >&2
  exit 1
fi
RUN_ROOT=$(mktemp -d "${BENCH_ROOT%/}/rocksdb-preallocate.XXXXXX")
COMMON=(
  --benchmarks=fillrandom,compactall,flush,waitforcompaction,stats
  --num="$TOTAL_WRITES" --writes="$((TOTAL_WRITES / THREADS))"
  --threads="$THREADS" --batch_size=1 --seed=1001 --duration=0
  --key_size=16 --value_size=1024 --compression_type=none
  --write_buffer_size=67108864 --max_write_buffer_number=4
  --target_file_size_base=67108864 --max_bytes_for_level_base=268435456
  --level0_file_num_compaction_trigger=4 --max_background_jobs=2
  --disable_auto_compactions=0 --use_existing_db=0
  --options_file=""
  --disable_wal="$DISABLE_WAL" --sync="$SYNC_WRITES" --use_fsync=0
  --use_direct_io_for_flush_and_compaction="$DIRECT_SST"
  --bytes_per_sync=0 --wal_bytes_per_sync=0
  --histogram=1 --statistics=1 --stats_interval_seconds=10
)
# 偶数轮反转次序；预热结果单独标记，不计入中位数。
for round in 0 1 2 3 4 5; do
  if (( round % 2 == 0 )); then modes=(0 1); else modes=(1 0); fi
  for mode in "${modes[@]}"; do
    case_dir="$RUN_ROOT/round-$round-mode-$mode"
    mkdir -p "$case_dir"
    /usr/bin/time -v -o "$case_dir/time.txt" \
      "$DB_BENCH" "${COMMON[@]}" \
      --db="$case_dir/db" --wal_dir="$case_dir/db" \
      --enable_zero_range_preallocation="$mode" \
      > "$case_dir/bench.log" 2>&1
  done
done
echo "$RUN_ROOT"
```

当前 DoWrite 按每个线程的 writes 参数执行，模板因此将总写入数除以线程数。仍须核对最终报告的实际操作数，而不是只看 `--num`。随机填充允许覆盖 key，操作数不等于最终 key 数。

- sync 场景设置 `SYNC_WRITES=1`，其余相同，必要时预先固定更小的总量，A/B 不能不同。
- SST 场景设置 `DISABLE_WAL=1`，保持 `SYNC_WRITES=0`；必须确认没有 WAL 写出。
- Direct I/O 设置 `DIRECT_SST=1`。
- fillseq 改 benchmark 名并固定 THREADS=1。
- overwrite 使用独立初始化阶段：先创建并关闭种子库，再为每次运行复制到新目录，使用 `--benchmarks=overwrite,flush,waitforcompaction,stats --use_existing_db=1`。种子库的绝对 wal_dir 不能指回共享目录，复制后打开时显式指定本副本 wal_dir。

默认不清系统 page cache；需要冷缓存试验时另列组，保持两侧同样方法。设置压缩、限速或改变内存大小时重新建立独立的对照，不与上述结果混算。

### 13.4 运行路径证据

先做独立的小规模诊断运行，减小 write_buffer 和 target SST 粒度，让 WAL、Flush、Compaction 都发生。使用 strace 的 `-ff -yy` 与时间信息，追踪：

```text
openat, fallocate, write, pwrite64, ftruncate, fdatasync, fsync, close
```

确认：

1. 模式 1 的新建 `.log`/`.sst` 有 `FALLOC_FL_ZERO_RANGE`，不带 KEEP_SIZE；非目标文件没有误用。
2. 从 LOG 的 flush/compaction job 及文件编号确认至少一份 Flush SST、一份真正 Compaction 输出 SST；不能只凭 `.sst` 后缀推断来源。
3. 同一 fd 后续分配区间不覆盖先前成功写出的数据，B/P 水位正确。
4. SST 完成包含 ftruncate，随后有真实同步，再关闭并安装；Direct I/O 不能截断后又写补齐尾页。
5. WAL 轮转后旧文件和正常关闭后的最后一个文件均裁到有效长度。
6. ZERO_RANGE 成功计数大于 0，fallback=0；否则不能把结果归为 ZERO_RANGE 性能。

诊断 tracing 不与正式性能运行混用。正式测试保留 DB 统计及低开销系统监控，例如 iostat/pidstat；trace 的额外成本不计入正式 A/B。

### 13.5 指标、后台 drain 与数据校验

必须同时记录：

- 前台：实际写操作数、ops/s、MiB/s、平均延迟、P50/P95/P99/P99.9、CPU。
- 后台：Flush/Compaction 输出字节、次数、耗时、stall、各 level 文件数和待处理工作。
- 文件：分配/裁尾/同步次数与耗时、峰值磁盘空间、最终空间、错误与 fallback。
- 全流程：写入结束、显式 Flush、后台 drain、DB Close 的耗时；最终磁盘写入量。

模板中的 `waitforcompaction` 会固定等待约 5 秒，并通过可能有竞态的轮询检查后台属性；因此它是辅助证据，不能单凭它证明精确 drain 时长。正式验收需在 db_bench 的 flush/wait/关闭阶段加明确计时，关闭前连续采样确认运行中的 Flush/Compaction、pending 标志和 immutable memtable 均为 0，并记录 timeout/错误。A1/B 使用相同计时代码；A0 没有该补充时至少保留进程总耗时，不能伪造其分阶段数字。

最后一次 benchmark 的 stats 发生在 DB 关闭前，无法包含最后一个 WAL 的裁尾和同步。性能验证实现必须显式检查 DB::Close() 返回值，并在关闭后补报保留的统计对象及关闭耗时；不要只依赖析构关闭。新增 flag 赋值、最终统计和关闭错误检查是性能验证代码的交付项，不以当前 db_bench 恰好输出了 stats 代替。

当前 db_bench 的进程总耗时含等待和关闭成本，不等于前台写入时间。前台吞吐用对应 write benchmark 的统计；端到端吞吐用实际写操作数除以定义一致、包含收尾的时间。固定等待开销单列，不偷偷从某一侧扣除。仅前台变快、后台债务明显增加，不判定整体提升。

正式计时结束后再运行离线 checksum、重开数据库和数据比对，不把验证读取计入只写吞吐。对 overwrite/fillrandom 使用相同随机种子与线程数，另做可确定预期内容的功能用例；多线程操作顺序不确定时不能把值逐字相同作为唯一正确性条件。验证应至少包括成功写入数、无错误、完整扫描/校验、相应持久性承诺，以及确定性测试的预期键值。

### 13.6 结果报告与验收

记录每次原始输出，中位数及离散程度，延迟百分位直接来自每轮分布，不能对几个 P99 做出未经说明的“总体 P99”。建议结果表：

| 场景/FS/线程 | A0 ops/s | A1 ops/s | B ops/s | B/A1 增幅 | A1/B P99 | 收尾及 drain | 分配/裁尾成本 | fallback | 结论 |
|---|---:|---:|---:|---:|---|---|---|---:|---|
| 报告模板，实际数据见第15节 | — | — | — | — | — | — | — | — | 按实际场景判定 |

```text
吞吐增幅 = (median(B.ops/s) / median(A1.ops/s) - 1) × 100%
延迟降幅 = (1 - median(B.latency) / median(A1.latency)) × 100%
```

只有正确性通过、三条关键路径实际命中、无 fallback，且改善超过运行噪声，才能声称提升。建议将主场景吞吐提升至少 5% 作为初始工程目标，同时要求 P99、关闭成本和峰值空间没有不可接受的退化；5% 是目标，不是已有结果或用户给定承诺。若置信区间跨越零或不同场景方向相反，报告“不显著/仅在指定场景改善”，不能选择性只展示有利结果。

性能没有提升也应如实交付：分清 EOF 提前扩大、Flush 新增预分配、ZERO_RANGE、本次新增同步分别贡献的成本。可另做同粒度 legacy/模式 0 的诊断构建排查原因，但不把诊断 mode 当作主功能或混入主要 A/B。

## 14. 完成交付条件

- 默认配置在支持的 POSIX 路径启用 ZERO_RANGE，关闭开关和降级原因可验证。
- WAL、Flush、Compaction/Subcompaction，以及共用创建入口的恢复/Repair/外部 SST 全部接入。
- 成功文件最终长度正确；裁尾与同步顺序正确；失败输出不安装、不假报成功。
- GetUpdatesSince 原有停住再继续语义通过，底层 stdio 不缓存 R 之外的零尾部，小读取及同区间追加回归通过；WAL 恢复不把预分配尾部当有效数据，也不掩盖损坏。
- 多 CF、manual_wal_flush、two_write_queues、并发 sync、Direct I/O、fallback 具备明确测试结果；recycling 和跨进程追尾按第 2.3 节排除，不要求其测试或性能结果。
- 关键正确性通过后，完成 release db_bench A0/A1/B，只写数据、syscall 路径证据及原始日志齐备。
- 对性能是否提升给出实测结论；本次实现和优化的实测结论见第 15 节，不将场景相关或区间跨零的结果写成普遍收益。


## 15. 第二阶段：异步写与同步写共同优化（2026-11-04）

### 15.1 目标及不变条件

以 `cd7a6a37f` 为优化前基线，在默认开启 ZERO_RANGE、WAL/Flush/Compaction 全部覆盖、batch_size=1 的条件下改善异步写，同时保持同步写收益。性能提升是实测验收目标，不通过降低持久性承诺或只挑选有利结果达成。recycling、Secondary 的原有限制保持不变。

前期屏蔽所有 WAL fdatasync 的诊断吞吐提高约 5.65%，只说明同步成本值得优化；该实验没有保留正常关闭承诺，不能作为产品实现。正式实现只跳过确认将删除的旧 WAL 同步，或跳过已有成功同步覆盖的同一封口状态。

### 15.2 旧 WAL 的关闭策略

`logs_to_free_` 只表示 Writer 所有权可以释放。保持其原有含义，不以此推导文件已过期。

清理任务的快照同时满足以下条件，才允许关闭时不额外同步：

- 本实例拥有 WAL；未启用 WAL TTL/大小归档；
- 文件编号明确在该任务 `log_delete_files` 中，低于 `log_number`，不等于 `prev_log_number`；
- 文件不在该任务 recycling 列表中；上述名单由持有 DB mutex 的 FindObsoleteFiles 产生，沿用已持久化 SST/MANIFEST 的最小存活 WAL 判断。

不新增异步删除判定，不把 full scan 中所有较小编号文件直接视为可跳过同步。未知归属、仍存活、禁删或需归档的文件均走保守同步路径。立即清理与后台关闭队列使用同一判定，队列显式携带该任务的同步要求；正常 DB::Close 对其仍拥有的 Writer 继续要求同步。

跳过同步仍执行 WriteBuffer → Seal/Truncate → Close，并保留首个错误、释放 fd、记录 sticky WAL 错误。Direct I/O 的既有 Close 同步语义继续保留。删除失败不使 WAL 再次成为恢复依赖：成功持久化的 SST/MANIFEST 已覆盖其恢复职责，遗留文件按既有协议清理。

### 15.3 封口状态的同步记账

新增仅针对 sealed 文件的成功同步状态，区分 fdatasync 与 fsync。只在实际系统调用成功、且该同步覆盖 Seal 之后的状态时发布。对未封口文件的同步不能覆盖未来裁尾；失败不记成功。正常 Close/FinishAndClose 只在所需同步级别尚未满足时执行系统调用。

SyncWithoutFlush 允许并发 Append，不写 pending_sync_ 等单 Writer 状态；封口后的同步完成标记用原子状态发布。同步与轮转继续使用 getting_synced/log_sync_cv_，确保 Seal 不与已经开始的同步交错后误记成功。必要的目录同步不受此状态替代。fdatasync 必须持久化读取所需的文件长度变更，fsync 成功可以覆盖 fdatasync 要求，反向不可推断。[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)

### 15.4 Append 快路径

首次实际扩容时验证并缓存固定粒度和 I/O 对齐；创建者必须在第一次 Append 前设置粒度，开始写后不动态调整。同一 Writer 即使实际 syscall fallback 也继续使用该已验证范围算法。

对非空追加先判断 `offset <= reserved_end && len <= reserved_end-offset`，只在不满足时读取/验证首次配置及计算新末尾。减法条件防止整数溢出。reserved_end 是 I/O 对齐后的成功分配水位，因此 Direct I/O 在范围内追加后的补齐末尾不会超过 reserved_end；扩容仍以已对齐水位为起点，不覆盖之前写入的尾页。无效粒度、off_t 溢出、分配失败的错误语义保持不变。

### 15.5 正确性与性能验证

新增测试覆盖：封口前 Sync 后 Close 仍需同步截断；封口后 Sync/SyncWithoutFlush 成功避免重复同步；失败不得复用；fdatasync 不替代 fsync；多次追加只在首次读取配置；跨界和 Direct I/O 补齐保持内容与分配范围正确。

DB 测试通过带真实 POSIX 目标的统计 FS 记录每份 WAL 的 truncate/sync/close/delete/archive，覆盖普通过期删除、归档、禁删、SyncWAL 释放但仍存活、正常关闭和后台清理；并回归 sync=true 崩溃恢复、多 CF、two_write_queues、manual_wal_flush、并发 SyncWAL/轮转及错误上报。

正式性能至少比较：原始 A0、新实现 flag=0、cd7a6a37f flag=1、优化后 flag=1；优化前后二进制、共享库和哈希独立保存。每组 1 次预热加 5 次测量，1/4 前台线程，后台 jobs=2。先复用前次主场景参数，再补充同操作数/同文件粒度的 sync=0/1 成对实验，不能仅比较两种不同几何的收益方向。保持随机种子、总操作数、数据尺寸、编译选项及设备一致。记录前台吞吐、P99、CPU、drain/close 和端到端耗时，检查全部输出 checksum/完整扫描及 ZERO_RANGE 实际命中三条路径。

成本拆分是独立诊断：固定新 Writer 的粒度、裁尾和同步策略，只改 Allocate 的 KEEP_SIZE/ZERO_RANGE；按 WAL/Flush/Compaction 分别切换并验证命中，不增加生产配置开关。诊断构建与正式默认开启结果分开报告，最终产品覆盖范围不缩减。另以同一个探针成对测量 zero/skip_flush：后者仅在 buffered I/O 的独立诊断进程模拟 Flush Allocate 成功而不调用内核，保留同一 Writer 的范围记账、封口和同步策略，用于区分新增 Flush 预分配的成本。实际跳过次数单列，绝不把它计为真实 ZERO_RANGE 成功，也不将该模式交付为产品配置。

WriteBatch 8/16 是独立提交方式实验，不作为 batch=1 的验收替代。db_bench 只能测给定批次的提交服务时间，不能据此推断业务组批等待；没有实际到达/组批模型时明确注明未测组批等待。


### 15.6 基准计时修正

2026-11-04 首轮正式运行发现宿主环境的 gettimeofday 数秒跳变，db_bench 的 Stats 使用该时钟，导致前台 ops/s 同样受影响。该轮 `.cache/zero-opt-async` 标记无效且保留，不用于最终结论。合成 +60 秒跳变用例复现：实际进程单调时间约 0.226 秒，原 Stats 却报告写入约 60.168 秒。

最终所有组的 db_bench Stats 均使用 `SystemClock::NowNanos()/1000`（当前 Linux 实现为 CLOCK_MONOTONIC）计算间隔、吞吐和操作延迟；sine 速率消费者使用相同时间域，日历时间输出仍取 wall clock。新增的 Flush/drain/close 计时也使用单调时钟。A0 和优化前 P 仅重新编译相同的基准计时改动，分别使用对应版本头文件和冻结未改动的 RocksDB 共享库；因此最终 A0 是“原始数据库实现 + 相同基准计时修正”，不同于首轮未经修改的 A0 可执行文件。源文件、编译命令、原始及新哈希保留。

Python runner 的端到端计时使用 CLOCK_MONOTONIC 和 pidfd 等待进程完成；GNU time 原始输出同时保留。库内部统计时间仍按原有时钟产生，若遇时钟调整不据其单个异常时间作因果结论；实际 fallocate 成本探针另用 CLOCK_MONOTONIC。正式配置固定 duration=0、report_interval_seconds=0，不使用依赖 wall clock 的限时负载或周期吞吐报告线程。

### 15.7 本轮实现与验收结果

旧 WAL 删除快照判定、后台队列同步要求、封口同步级别复用及 Append 减法快路径已实现。1069 个选定测试通过，包括真实 POSIX 多 CF 保留 WAL 的四种 manual/two_write_queues 组合。普通旧 WAL 删除案例最终同步从 38 次降至 28 次，裁尾保持 38 次；仍需恢复、归档、正常关闭及同步失败的行为保持承诺。

完成 228 个正式/成本诊断 case（含预热），每个均做离线 checksum 和重开完整扫描；另对最后一轮 31 份数据库校验 key/value 长度、键集合摘要，单线程组核对键值摘要。每个正式组 1 次预热和 5 次测量，前台最多 4 线程，后台 jobs=2。

以下为同一优化后二进制开启相对关闭 ZERO_RANGE 的吞吐中位数变化；不将不同规模/粒度的同步、异步结果直接相减：

| 场景 | 线程 | 开启/关闭增幅 | 95% bootstrap 区间 | 开启后相对优化前 |
|---|---:|---:|---|---:|
| 异步，64MiB/60万次 | 1 | -9.85% | [-11.85%, +3.93%] | -8.07% |
| 异步，64MiB/60万次 | 4 | -6.44% | [-17.28%, +9.40%] | -2.69% |
| 异步，64MiB/300万次 | 1 | +0.98% | [-8.00%, +12.17%] | -3.05% |
| 异步，1MiB/1万次 | 1 | +4.22% | [-33.66%, +44.06%] | +13.69% |
| 异步，1MiB/1万次 | 4 | +0.01% | [-11.36%, +4.55%] | +8.52% |
| 同步，1MiB/1万次 | 1 | +62.71% | [+48.55%, +74.56%] | -1.79% |
| 同步，1MiB/1万次 | 4 | +30.42% | [+16.54%, +38.31%] | +0.15% |

同步写相对关闭状态有明确收益；相对优化前开启版本，本次额外吞吐增幅不明确。异步各组区间跨零，短组点估计偏低，长组接近持平，频繁轮转组方向为正但噪声较大。**本轮未全面达到“开启 ZERO_RANGE 后异步与同步均稳定提升”的性能目标。** 不通过减少默认覆盖、降低正常关闭持久性或挑选有利样本改变结论。

同 Writer/粒度/收尾策略的 KEEP_SIZE/ZERO_RANGE 及分来源标志诊断已完成。额外 zero/skip_flush 成对诊断中，跳过 Flush 分配也未呈现稳定正向收益；不能只凭分配 syscall 累计耗时推导整体吞吐原因。因此本轮不据此改动生产粒度。独立 batch=8/16 结果只作业务提交方式实验，未模拟组批等待；不同批量区块不可直接用作最优批量结论。

原始日志、每轮参数/哈希、P99/CPU、drain/Close/端到端及空间数据见 `.cache/zero-opt-validation/report.md` 和各组 `analysis.md`、`metrics.csv/json`。源码复现工具为 `tools/benchmark_zero_range.py`、`tools/summarize_zero_range.py`、`tools/benchmark_zero_range_costs.py` 与 `tools/zero_range_cost_probe.c`。首轮受 wall clock 跳变影响的运行保留并明确排除，未与修正后的结果合并。
