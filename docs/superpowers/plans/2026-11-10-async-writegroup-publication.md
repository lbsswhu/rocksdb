# WriteGroup publication implementation plan

> **For agentic workers:** Use superpowers:executing-plans inline in the existing worktree. The user approved the written design and explicitly requested implementation; proceed through all tasks without another permission gate.

**Goal:** Hand off complete owned WAL groups, publish each group promptly, and move multi-worker prefix publication to the sequence notifier.

**Architecture:** The WAL leader prepares owned storage before WAL and publishes one node per successful group to the existing lock-free FIFO. One worker applies every range in that group. A single worker publishes directly; multiple workers transfer preallocated completion records to the notifier, which alone maintains the ordered completion list and retires the continuous prefix.

**Tech Stack:** RocksDB C++11, existing hazard-protected FIFO, epoch/futex events, CMake Debug and ASAN/UBSAN, gtest.

**Spec:** `docs/superpowers/specs/2026-11-10-async-writegroup-publication-design.md` (approved design copied from the discussion artifact).

**Follow-up:** This records the initial whole-group implementation. The later single-worker plan acquires the ready prefix, publishes each group before batch-end cleanup, and keeps the fetch limit for multiple workers only. Synchronous WAL handoff now notifies followers before releasing the gate; non-sync handoff stays leader-first. The initial constraints below describe that earlier implementation stage.

## Global Constraints

- Preserve caller-independent owned payload and reuse it for WAL and memtable; no payload copy or node allocation at post-WAL submission.
- All worker counts use whole-group tasks. `async_commit_memtable_groups_per_fetch=1`, immutable, range [1,64]; consume available groups without waiting to fill the limit.
- Complete each group before applying the next locally fetched group. Do not cross an exclusive Merge fence while prefetching.
- Single worker directly publishes continuous P. Multi-worker completion records are release/acquire transferred and merged by notifier.
- Keep reader A, spin defaults 30/6/50, slot mutex/CV registration and fixed notification snapshots.
- Resource cleanup precedes P/D; release heavy resources in worker, not notifier. Retain metadata accounting for out-of-order groups.
- Error freezes P, but cancellation and D still drain. Successful drain requires published and cleaned group tickets, including zero-record groups.
- Next WAL leader progresses promptly after queue publication. Ordinary writes and worker=0 retain their existing paths.
- Performance testing requires an advance reminder. This implementation phase runs correctness tests only.
- Keep changes reviewable without a new product commit unless the user requests another checkpoint.

## Review Focus

- WAL failure after preparation must not leave a self-owned completion record or a group-id hole.
- Completion records cannot outlive their ownership or be freed while producers/notifier touch them.
- Pending metadata must remain bounded while an earlier group or notifier is stalled.
- Prefetched work cannot deadlock an exclusive Merge fence that depends on that work.
- Reader slot notification must occur outside the publication/accounting mutex, including error paths.

## Task 1: Whole-group work and configurable fetch

**Files:** `db/async_commit_executor.{h,cc}`, `db/db_impl/db_impl_async_commit.cc`, `util/async_commit_event.h`, DBOptions/ImmutableDBOptions/option helpers, `tools/db_bench_tool.cc`, associated tests.

**Interfaces:** `AsyncCommitTask` identifies one group; `AsyncCommitEvent::NotifyOne()` is work-ready only. Add uint32 option `async_commit_memtable_groups_per_fetch` default 1 with [1,64] validation and OPTIONS/tool round trip.

- [x] Generalize the existing owned-group test to every worker count; replace same-group parallel range expectations with sequential same-worker execution, while retaining separate-group parallelism coverage.
- [x] Strengthen early publication to require P >= 16 while group 17 is stalled, rather than merely P >= 9. Demonstrate both failures against the baseline.
- [x] Add configuration validation/roundtrip and non-default fetch coverage. Verify expected RED before implementing each behavior.
- [x] Prepare one queue node per group, apply all ranges using the configured memtable concurrency policy, fetch at most the configured group count, and complete every group immediately.
- [x] Wake one worker per submitted group; keep all-worker wake for stop/error. Preserve epoch register/recheck/wait.
- [x] Run targeted tests, queue tests and option/tool tests with nonzero counts; record results.

## Task 2: Notifier completion ownership and prefix

**Files:** executor, `db/async_commit_sequence_notifier.{h,cc}`, DBImpl integration, executor tests.

**Interfaces:** `AsyncCommitSequenceNotifier::SubmitCompleted(const std::shared_ptr<AsyncCommitGroup>&)` transfers a preallocated completion record; notifier calls executor retirement under async mutex. Executor keeps `drained_ticket_` and `published_ticket_`; no pre-WAL completion registry is required.

- [x] Add a deterministic test that stalls the notifier before consuming reports: multi-worker P and drain must stay pending while a worker can apply another group; single-worker publication must remain independent.
- [x] Add out-of-order complete/metadata-group prefix, completion-before-sleep and terminal-error drain assertions. Demonstrate baseline failure for notifier ownership.
- [x] Assign group ids at successful queue submission; synchronously clean unsubmitted prepared failures without registering background ids.
- [x] Keep a self-owned completion reference prepared before WAL for multi-worker tasks; release-transfer through a raw-pointer MPSC completion chain. Notifier consumes ownership and inserts task-owned nodes into an ordered list.
- [x] Process at most 64 reports per notification cycle; shrink released payload accounting, retire contiguous group ids, update P/D/tickets under the existing error mutex, notify management CV after unlock.
- [x] In multi-worker mode consume completion progress even without reader waiters; in single-worker mode retain idle waiter-driven notifier behavior.
- [x] Adapt deterministic notifier tests that previously assumed P could advance while the multi-worker notifier was deliberately blocked: resume notifier before waiting on its publication/drain, retaining equivalent race assertions.
- [x] Run focused ordering/ownership/close/error/fence tests and the complete executor suite.

## Task 3: Integration, review and final verification

**Files:** DBImpl WAL handoff, write thread, remaining affected tests, design/progress artifacts.

- [x] Use the whole-group post-WAL handoff for sync=false/true, preserving required WAL sync before enqueue, safe Writer detach and immediate next leader. Extend masked WAL completion event to all pool configurations if required by the unified handoff.
- [x] Verify automatic flush, CF Drop, caller buffer reuse, quota, Merge fences, both visibility modes, zero workers, ordinary writes and DB close with all existing suites.
- [x] Build Debug and Release, run complete affected Debug suites, then ASAN/UBSAN executor coverage. No build/test overlaps a benchmark.
- [x] Request one independent read-only whole-change code review under the executing-plans/requesting-code-review skills; fix material findings with regression evidence.
- [x] Check diff formatting, actual source/binary configuration, test counts and final pending/error state. Report implemented behavior, limits and validation; do not claim performance improvement without measurements.

## Execution record

Use `.superpowers/sdd/2026-11-10-async-writegroup-publication/progress.md` for task state, rulings and evidence; preserve logs under `build/async-writegroup-implementation-20261110/`.
