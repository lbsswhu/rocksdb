# Async commit WAL/memtable separation implementation plan

> Historical plan: the original pipelined=false leader-wait rule was a design error, superseded by the user correction in docs/design/async_commit.md section 22.18. The executor now acknowledges all callers after owned enqueue regardless of the native pipelined flag.

> **For agentic workers:** Use superpowers:executing-plans for inline implementation, test-driven-development and verification-before-completion. Review the whole change with one fresh reviewer at the end.

**Goal:** Implement docs/design/async_commit.md section 22 in the existing async-commit-worktree.
**Architecture:** DB-owned ThreadPoolImpl drain loops take owned request ranges from ordered WAL groups. Insertion may finish out of order; publication and cleanup advance contiguous group prefixes. WAL gate protects management drains.
**Tech Stack:** C++17, RocksDB 6.26.1, gtest, SyncPoint, CMake, ASAN/UBSAN/TSAN.
**Spec:** docs/design/async_commit.md section 22.

## Global Constraints
- worker=0 preserves V1; no changes to global yield defaults.
- workers>1 requires allow_concurrent_memtable_write; worker=1 permits serial Add.
- pipeline=false retains WAL gate through application; pipeline=true returns all callers after owned enqueue.
- owned single-group payload, captured CF/memtable targets, no retained caller pointers.
- whole-group contiguous publication; separate cleanup drain after errors; bounded bytes and group quotas.
- Merge exclusive fence; private inserter/cursor; PostProcess before completion; scheduler under DB mutex.
- no DB mutex during drain/capacity waits/join; worker never enters WAL gate.
- no commit, push, or merge; preserve existing uncommitted V1 work.
- current performance comparisons remain serial-only; future worker scaling measurements are separate.

## Review Focus
- return/clear caller batches before application, including checksums and multiple CF.
- first group delayed while later same-key/range-delete groups complete.
- terminal errors while ranges are running and management drains wait.
- flush/WBM/CF mutations/close with acknowledged work in flight.
- zero-record groups, capacity and no_slowdown, exclusive Merge fences.

### Task 1: Owned executor and lifecycle
Files: db/async_commit_executor.{h,cc}, db/db_impl/db_impl_async_commit.cc, db_impl.{h,cc}, db_impl_open.cc, write_thread.{h,cc}, option plumbing and build lists.
Interfaces: DBImpl::DrainAsyncCommits(uint64_t), executor SubmitPreparedGroup/DrainThrough/Shutdown; WriteThread::ExitAsyncWalGroup; WriteThread unbatched drain callback.
- [x] RED: options parse/roundtrip, serial pool early WAL acknowledgement + owned data + close drain tests.
- [x] GREEN: implement options, owned group queue, fixed worker loops, WAL detachment and open/close/drain hooks.
- [x] Verify focused pool tests and all V1 tests; record actual results.

### Task 2: Concurrent range insertion and prefix publication
Files: executor, write_batch.cc/write_batch_internal.h, memtable.cc if required; db/async_commit_test.cc.
Interfaces: InsertIntoRange(..., bool concurrent=false); task-private target adapter; ordered completion on shared mutex.
- [x] RED: out-of-order same-key/cross-CF publication, intra-group parallelism, partial insertion failure, DeleteRange, Merge fence and concurrent metadata.
- [x] GREEN: fair bounded leases, captured targets, concurrent Add/PostProcess, exclusive Merge fence, scheduler completion and terminal cancellation.
- [x] Verify targeted concurrency tests and V1 regressions.

### Task 3: Management/backpressure/correctness coverage and benchmark drain
Files: management write paths, executor, db_bench_tool.cc, pool tests and design status.
Interfaces: management unbatched/WaitForPendingWrites drain; completed benchmark time includes explicit drain.
- [x] RED: zero-record group quota, no_slowdown, flush/switch/CF/SetOptions/ingest barriers, terminal cleanup, close, WAL recovery and TLS read waits.
- [x] GREEN: finish audited drains, pre-stall scheduling, metrics, benchmark drain and documentation.
- [x] Verify Debug, ASAN/UBSAN, TSAN and relevant existing regression binaries; real nonzero counts.
- [x] Final whole-change review, resolve important findings, rerun necessary checks; report evidence and limits.
