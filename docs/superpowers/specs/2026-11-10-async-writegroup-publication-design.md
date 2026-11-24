# WriteGroup 交接与连续发布设计

基线：`785a62dc6`。用户已批准本设计并要求实施；性能测试前另行提醒。

后续实现更新：下文保留最初的整组交接设计。单 worker 后续改为获取当前已就绪前缀，逐组 apply 后立即发布 P，批末统一清理资源和推进 drain；领取参数只约束多 worker。多 worker 仍在清理后报告完成，由 notifier 合并连续前缀。同步写已恢复为先通知本组 followers、再释放 WAL gate，非同步写继续先交接下一位 leader。单 worker 的后续方案见 `docs/superpowers/plans/2026-11-11-async-serial-batch-publication.md`。

## 用户已明确的目标

同一个 WriteGroup leader 完成 WAL 后，将整组任务交给 memtable worker；交接尽量避免内存拷贝，随后直接通知本组 followers 并交接下一位 WAL leader。worker 默认一次领取一个 WriteGroup，领取数量可配置。sequence notifier 使用 list 处理乱序完成区间，只在连续可见前缀前进时通知 reader。

这里的共享队列是 WAL leader 生产、memtable workers 消费的工作队列。WAL writer 不再从这个工作队列消费一次；也不新增 WAL 后台线程。前台 Writer 的排队与组装仍承担合并调用者的职责。

## 基线 785a62dc6 的落点（改造前）

- `db/db_impl/db_impl_async_commit.cc:532`：原生 WriteGroup 是 leader 栈对象，内部指向调用者 Writer；owned AsyncCommitGroup 与调用者生命周期独立。
- `db/db_impl/db_impl_async_commit.cc:448`：先构建独立持有的 merged batch，然后同一份 batch 写 WAL 和 memtable。Append 接管数据发生在 WAL 前，交接本身没有再次复制 payload。
- `db/async_commit_executor.cc:49`：当前仅一个 worker 时每组一个节点；多个 worker 仍拆成 range 节点。
- `db/async_commit_executor.cc:236`：当前单 worker 会领取多个队列项，再按目标 8 range 分块完成。
- `db/async_commit_executor.cc:341`：当前资源释放、连续前缀、配额和 drain 在 Complete 内串行组织。
- `db/async_commit_sequence_notifier.cc:64`：当前 notifier 读取已经连续的 LastSequence，只负责通知，并不合并 worker 完成区间。

## 工作队列与所有权

所有 worker 数量统一使用一个完整组任务，不再把一个 WriteGroup 分给多个 worker。不同 WriteGroup 可以并行；一个组内部保留原 Writer range 的顺序、sequence、CF 与选项。

可复用 AsyncCommitGroup 作为稳定任务描述符；不能把栈上的 WriteThread::WriteGroup 指针直接提交给后台。任务持有 owned batch、range 元数据、CF/memtable 引用、group_id、sequence 边界、屏障信息与完成记录。

WAL 前完成任务、队列节点和完成记录的准备、配额预留与必要校验。WAL append，以及 sync=true 所要求的同步和记账，全部成功后才向工作队列发布。可排空的 group_id 在成功提交点赋值：准备失败的任务仍是未提交对象，由当前 owner 同步释放资源和配额，不在后台完成序列中制造空洞。已经遇到后台终止错误但本组 WAL 已成功的任务仍必须有确定的取消/清理去向。

交接只发布任务节点/所有权，不复制 key/value，不在 WAL 完成后重新合并 batch，也不在该阶段等待容量。发布后 leader 不再读取可能被 worker 清理的 batch/range/target 字段。

使用现有单逻辑生产者、多消费者的 hazard-protected FIFO；WAL gate 已将各 leader 的生产操作串行化，无需先替换为多生产者工作队列。

严格的调用者到后台全程零拷贝不能由现有非消费式 Write(WriteBatch*) API 保证：调用者在 weak Put 返回后可以清空、复用或销毁 batch。第一版保留 WAL 前的一次 owned payload 构建，确保 WAL→memtable 交接零额外 payload 拷贝。不得通过借用调用者 batch 或隐式移动调用者内容实现。

## WAL 完成与前台通知

正常路径：

1. Prepare owned task。
2. 分配 sequence，写 WAL，完成所需同步。
3. 向工作队列发布一个完整组任务。
4. 唤醒需要工作的 memtable worker。
5. 修复/摘除旧 Writer 链并立即交接下一位 WAL leader；合并通知本组 followers。
6. weak 调用者在 WAL 成功后返回；strong 调用者继续等待自己的目标 sequence 达到全局 P。

实际实现必须先完成安全摘链，再对任何 Writer 发布允许返回的最终状态。发布后不可再次访问该 Writer。只交接下一位 leader，不同时唤醒所有后续候选 leader。

work-ready 事件增加单个唤醒能力；默认一个新组最多唤醒一个休眠 worker。关闭和错误保留全体唤醒。保留 epoch 注册→重检工作→等待的防丢唤醒协议。

## worker 领取与完成

配置名：`async_memtable_write_groups_per_fetch`，默认 1，第一版范围 [1,64]，DB 打开后固定。含义是一次最多领取当前已经就绪的 N 个完整组；不等待凑满 N，不跨越 Merge 独占屏障。DBOptions、OPTIONS 解析/输出和 db_bench 参数一致接入。

即使 N > 1，也必须依次对每组执行 apply→报告该组完成，然后处理下一组，不能恢复为整批 apply 后统一报告完成。领取数量与发布时机分开。

一组所有 range 全部成功，才能报告该组的完成区间。worker 完成位置不是任意可直接写进 LastSequence 的最大值。

## P 的职责划分：用户已选择

P 始终表示全局连续可见前缀。reader 和 strong Put 仍 acquire 读取它。已完成区间与 P、reader 通知游标 N 分别表示。

### 单 worker 直接发布，多 worker 由 notifier 合并

单 worker 严格按组顺序执行，成功完成的普通组没有跨 worker 空洞，可以直接推进 P 并发出 notifier 进度事件；notifier 只处理 (N, P] 的 reader 通知。错误冻结、Merge 屏障和零记录组仍按明确协议处理，不能无条件裸 store。

多 worker 完成后发布该组的完成记录；notifier acquire 接收记录，合并连续区间，再 release 更新 LastSequence，并通知 reader。worker 不再争用同一把锁遍历成功前缀。

用户已明确选择该划分。单 worker 保留直接发布路径，多 worker 移出成功前缀扫描。两条路径必须共用同样的终止错误裁决规则。

### 未采用的替代：所有配置统一由 notifier 推进 P

所有 worker 只发布完成记录；同一套 list 逻辑负责连续性、P 和 reader 通知。

实现与证明更统一，但单 worker 的 apply→P 也必须经过 notifier 调度；notifier 正在等待 reader slot mutex 或执行通知时，下一轮 P 也会被推迟。不能假设它比选择 A 更低延迟。

## notifier 的区间 list

只有 notifier 修改乱序完成 list。第一版使用任务自身预留的链表节点维护有序 list；节点在 WAL 前准备并计入元数据预留，不额外构建区间对象。worker 不获取 list mutex，也不直接插入 notifier 的 list。

worker 使用任务中预留的完成记录发布到多生产者、单消费者的完成通知链。该链只传完成元数据及其必要生命周期引用，是完成报告通道，不是第二条 memtable 工作队列。具体回收协议必须保证 worker 发布后记录不会先于 notifier 消费而销毁。

notifier 先处理能够直接接上当前连续前缀的完成记录：推进候选 P，再吸收 list 中已缓存的相邻区间。不能接上的完成区间插入有序 list。第一版每个节点保留完整 group_id、sequence 和预留元数据记账，不要求合并后立刻释放组描述符；连续区间合并体现为一次推进候选 P，避免丢失逐组配额和 drain 信息。单 worker 直接发布模式不经过这条乱序路径。

示例：P=100，G1=[101,110] 尚未完成。G2=[111,120] 完成后缓存；G3=[121,130] 完成后缓存的两个相邻节点共同覆盖 [111,130]；此时不提高 P、不通知 reader。G1 到达时吸收已缓存区间，一次将 P 推进到 130，通知 (100,130]。

不能只判断新到区间是否直接扩展 P：补洞后还必须继续吸收此前缓存的后续区间。序号不连续时不得通过取最大值跳洞。

list 按 group_id 排序，记录同时包含 sequence 边界。仅按 sequence 无法表示零更新、LogData-only 和失败取消记录；这些组仍参与有序完成与 drain。只有对应 group_id 连续、成功状态明确的区间才允许合并。

发布同步链为 memtable 修改→完成记录 release→notifier acquire→P release→reader acquire。错误状态与 P 推进必须串行裁决，避免在 terminal error 冻结后继续发布。

## reader 通知与 notifier 休眠

全局 P 前进后先固定快照 p，对 (N,p] 覆盖的所有 reader slots 服务一次，再令 N=p。保留 slot mutex 下登记、重检 P、进入 CV 的协议，跨越一整圈时覆盖全部 slots。strong Put 的慢路径也算 publication waiter。

多 worker 模式中，notifier 负责推进 P，故即使没有 reader 等待，也必须消费完成记录；只能省略 reader slot 扫描，不能沿用当前“没有 reader 就完全不处理完成进度”的策略。若采用统一 notifier 发布，单 worker 纯写也有这个要求。

notifier 已运行时合并进度，不按每组额外唤醒同一线程；休眠仍使用注册 epoch→重检完成记录/状态→等待。每轮最多消费 64 个完成记录，推进本轮候选 P 后服务 reader，再处理剩余记录，不能在写入持续到达时无限吸收完成消息而一直不通知。发布/管理锁必须在 reader slot 通知前释放，不能形成发布锁→slot mutex 与 reader 错误处理的反向嵌套。

## 回收、drain、错误和管理屏障

P 表示可见，D 表示资源安全排空；不能用 P 达标替代 drain。batch 容量、CF/memtable 引用释放完成后，才能回收对应配额并让 D 越过该组。额外保留已发布 group_id，使零记录组也有明确的发布/排空边界。

第一版保留当前资源释放先于 P/D 的顺序。worker 逐组完成 apply 和必要收尾，再报告该完整组；不再等待本次领取的其他组完成 apply。单 worker 在该组收尾后直接更新 P、配额和 D；多 worker 在收尾后把稳定完成记录交给 notifier，后者合并处理配额和有序 P/D 进度。资源释放保留在 worker，notifier 不承担 payload 释放和 CF/memtable 引用回收的重活。

因此，“及时发布”在本版指完整组完成后立即进入发布协议，不等整个领取批次，不等另一个成功组。它不等于最后一次 memtable 插入结束时就绕过资源收尾发布 P。若后续要继续把这段收尾移出 apply→P，需要另行拆分 APPLIED/CLEANED，而不能在本版顺带放宽 drain 保证。

多 worker notifier 处理到乱序完成记录时，可以先确认该组 payload 已释放并缩减其 bytes 预留；组描述符和完成记录的配额保留到有序退休，避免 notifier 堵塞时形成不受配额约束的完成积压。

正常成功的 DrainThrough(B) 必须确认清理前缀达到 B，且已发布 group_id 达到 B；终止错误时 P 冻结，仍等待清理前缀达到 B 后返回错误。不得把 worker 已经报告清理完成误认为 notifier 已处理发布。

memtable/WAL 切换、CF Drop、管理操作和 Close 继续获取相应 WAL 屏障并等待 D；管理等待期间释放 DB mutex。Merge 独占组等待前序 D，后序组等待 Merge 屏障。批量领取不得把完成前序工作所需的任务留在本地却先等待其后继屏障。

WAL 失败的准备任务不提交为成功应用任务；错误记录与预留必须被取消回收。任意应用终止错误冻结 P、唤醒读者；已经写入的后续组仍须取消/清理，使错误 drain 最终完成后返回错误。

关闭时保持 notifier 存活直至最后一组所需的发布与清理进度完成，停止并 join workers，再终止等待并 join notifier，最后销毁 versions 和 slots。若 P 推进依赖 notifier，不得先停止 notifier 再等待 workers 的前缀屏障。

## 验证重点

- 所有 worker 数都验证一个组只由一个 worker 应用；不同组可并行。替换当前期待同组 range 分散到不同 worker 的测试，而非跳过测试。
- followers 和下一位 leader 在 memtable apply 被阻塞时仍能完成 WAL 确认/继续写 WAL；weak 与 strong 分别断言。
- 调用者清空/复用 batch，后台仍读到独立 owned 数据；protected、多 CF、不同 range 选项与 metadata-only 保留。
- G2/G3 先完成不跨 G1 发布；G1 补洞后吸收已缓存区间。覆盖重复边界、零记录组、错误与取消。
- 配置为 N > 1 时，后组阻塞不得阻止前组完成报告；Merge 屏障不死锁。
- 完成报告与 notifier 休眠交错、最后一次进度、reader 登记、超时、slot 周期和关闭。
- 实际源代码、二进制、OPTIONS、计数、WAL 同步、P 与 D 全部可核验。
- 性能测试前提醒用户；再比较基线、独立原生和当前 async-off。分别量 Submit→Apply、Apply→完成报告、完成报告→P、P→reader 与资源清理，不用一个 queue 时间解释所有阶段。

## 本轮状态

用户已批准 WAL leader→整组工作队列→memtable worker 交接，以及单 worker 直接发布 P、多 worker 由 notifier 合并发布的职责划分。实现与验收记录见对应 implementation plan 和 build/async-writegroup-implementation-20261110/；性能测试前另行提醒。
