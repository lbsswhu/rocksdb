# Async commit V3 Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task in this session.

**Goal:** Implement the approved lock-free WAL-to-memtable handoff and compare native/V1/V2/V3 with serial memtables.

**Architecture:** Preallocate FIFO task nodes and register completion records before WAL. Publish an entire chain without the completion mutex; workers pop through hazard-protected CAS, poll while briefly idle, and use an epoch event when sleeping. Preserve group prefix publication, quotas, Merge fences, and management drains.

**Tech Stack:** Existing RocksDB C++11, pointer atomics, Linux futex, existing CMake/Make/gtest and sanitizer builds.

**Spec:** docs/design/async_commit.md, section 22.21 (approved by user).

## Global Constraints
- Existing async-commit-worktree only; preserve existing work. Implementation was initially kept uncommitted; on 2026-11-06 the user requested a local checkpoint commit of the current implementation. No merge or push is requested.
- Serial performance: allow_concurrent_memtable_write=false, one memtable worker, foreground threads=16, key=16 B, value=1024 B, batch=1.
- Performance: both sync and pipeline settings, 3 x 60 s per configuration, final drain included; preserve and actually load the V2 reference library.
- No allocation, capacity waits, or completion mutex acquisition in post-WAL submission.
- Ordinary tasks preserve original batch boundaries; Merge remains an exclusive barrier; A/P/D/R, cleanup-before-D, and terminal errors remain valid.

## Review Focus
- A consumer paused between pointer load/hazard publication/revalidation must not cause UAF or ABA.
- Notify in the last empty-check/sleep window must finish the final task without a subsequent write.
- Group preparation followed by WAL failure must remove its completion record and release quotas exactly once.
- A local batch must complete before waiting on a Merge barrier that depends on that batch.
- A queue dummy or retired node must not retain business payload or unbounded metadata after drain.

### Task 1: Freeze V2 and add deterministic behavior tests
**Files:** build/async-v3/v2-reference; db/async_commit_executor_test.cc.
**Interfaces:** Existing SyncPoint hooks plus new Submission/Completion/Idle hooks.
- [x] Archive db_bench/library/source and hashes; verify archived library via LD_LIBRARY_PATH.
- [x] Add SubmissionDoesNotWaitForCompletionMutex, PollingFindsWorkWithoutWake, and LastEnqueueCannotLoseWake tests, including cleanup on failed expectations.
- [x] Run new tests on old code and record real assertion failures before implementation.

### Task 2: Lock-free queue and idle event
**Files:** util/async_commit_queue.h, util/async_commit_event.h, util/async_commit_queue_test.cc; CMakeLists.txt/src.mk/Makefile test registration.
**Interfaces:** AsyncCommitQueue<T>::Node, PushChain(first,last), TryPop(worker,T*), Reclaim(worker); AsyncCommitEvent::PrepareWait/CancelWait/Wait/NotifyAll.
- [x] Add FIFO/chain/multiple-consumer/exact-once/paused-hazard and notify-before-wait tests; run RED.
- [x] Implement pointer-CAS FIFO with pre-registered hazard slots and bounded worker retirement lists; event-count idle sleep.
- [x] Run tests GREEN, including repeated contention and sanitizer checks.

### Task 3: V3 executor integration
**Files:** db/async_commit_executor.{h,cc}; db/db_impl/db_impl_async_commit.cc; tools/db_bench_tool.cc.
**Interfaces:** PrepareGroup(shared_ptr), CancelPreparedGroup(shared_ptr), SubmitPreparedGroup(shared_ptr), existing drain/property APIs.
- [x] Prepare/register tasks before WAL, charge storage, handle cancellation and immutable publication; leave V1 intact.
- [x] Replace shared-lock task dispatch with queue TryPop, bounded worker-local batches and spin/yield/event idle policy; keep completion registry separately locked.
- [x] Preserve Merge predecessor fences, error cancellation, prefix publication and pre-management drains; safely notify followers before WAL handoff.
- [x] Make deterministic tests GREEN; run all async tests and affected write/options/range/tool regressions.

### Task 4: Review and sanitizer verification
**Files:** build/async-v3 verification records; source fixes as needed.
- [x] Run Debug, ASAN+UBSAN, TSAN matrices and queue tests, prove nonzero counts and zero failures.
- [x] Obtain fresh-context whole-change review as required by executing-plans; resolve material findings with regression tests.
- [x] Preserve before/after evidence and source/binary hashes; no benchmark alongside builds/tests.

### Task 5: Performance and report
**Files:** build/async-v3/compare.py and result artifacts; docs/design/async_commit.md.
- [x] Build Release with the same options; preflight every mode and effective configuration, compare idle thresholds 10/50/100 us separately from formal data.
- [x] Run shuffled native/V1/V2/V3 x sync x pipeline x 3 x 60 s; verify mapped binary/library, operation counters and drained groups.
- [x] Analyze median/range, CPU, latency, WAL grouping and queue/sleep metrics; write results and limits to the design document and user report.
