# Async commit sequence notifier implementation plan

**Goal:** Move publication slot notifications off memtable workers without changing read visibility or spin/yield budgets.

**Architecture:** One independently joined notifier per async DB observes the continuous `LastSequence()` prefix P and services a private snapshot cursor N. A waiter event and a progress event let it sleep through weak pure writes. Workers retain resource reclamation, publication, quota accounting, drain tickets and management CV notifications.

**Spec:** `build/async-sequence-notifier-analysis-20261109/analysis.md` and the user's five approved design points.

**Execution:** Inline in the existing linked worktree, as requested. No additional approval or worktree is needed. Preserve the old Release executable/library before rebuilding. Leave changes reviewable without committing.

## Constraints and review focus

- Preserve global A capture, release/acquire publication, and reader spin=10 us/yield=40 us defaults.
- Include worker=0 and strong Put CV registrations; async disabled creates no notifier.
- Keep the notifier alive during worker drain; stop both events and join before destroying versions or slots.
- Errors directly wake all publication waiters even with frozen P.
- Never advance N while skipping an interval because the waiter count was zero.
- Cover full slot cycles, first waiter registration, last publication before sleep, publication during notification, and timeout deregistration.

## Task 1: Prove producer/notification independence

- [x] Add `WorkerContinuesWhilePublicationNotificationIsBlocked`, stalling the real slot notification after a reader registers, then submitting a second Put.
- [x] Run it against the old code: the second apply/publication must fail to arrive before notification is released.
- [x] Record the expected failure and preserve the original binary/library hashes.

## Task 2: Implement notifier and integrate lifecycle

Files: new `db/async_commit_sequence_notifier.{h,cc}`; update `db/db_impl/db_impl.{h,cc}`, `db/db_impl/db_impl_async_commit.cc`, `db/async_commit_executor.cc`, `CMakeLists.txt`, `src.mk`.

Interfaces: `Start(SequenceNumber)`, `Stop()`, `PublicationAdvanced()`, `RegisterWaiter()`, `UnregisterWaiter()`. DBImpl retains actual slot notification and adds a progress-signalling entry point. The notifier owns its independent thread, stop flag, waiter count and two AsyncCommitEvents.

- [x] Implement fixed-snapshot interval notification, leaving N unchanged in the no-waiter state.
- [x] Register global waiters only after the locked P recheck; RAII-unregister under the slot mutex on every exit.
- [x] Start before workers, including worker=0. Stop after worker drain and terminal reader wakeup.
- [x] Run Task 1 and the full async executor/async commit/queue test binaries.

## Task 3: Exercise notifier event and snapshot races

Files: `db/async_commit_executor_test.cc`.

- [x] Deterministically test progress between event registration/recheck/sleep, first waiter after idle, and P advancing while the notifier is blocked on a prior snapshot.
- [x] Test no slot scans on weak pure writes, timeout removal of the last waiter, both close sleep states, worker=0, and strong Put wakeup.
- [x] Retain existing multiworker holes, crossed-slot coverage, error and close tests. Run relevant stress repetitions and sanitizer checks where available.
- [x] Review the final diff and protocol against each approved requirement.

## Task 4: Performance and stage attribution

Files: reproducible scripts, raw logs, manifests and report under `build/async-sequence-notifier-20261109/`.

- [x] Build new Release binaries with the same compiler/options; keep original, independent native, and current async-disabled baselines.
- [x] Preflight effective OPTIONS, mapped executable/library, successful Get/Put counts, and complete async drains.
- [x] Serial randomized repeated tests: pure writes and 50 Get/50 Put mixed, WAL sync=false/true, memtable concurrency off with 1 worker and on with 1/2/4/8. Include native/off and readonly controls.
- [x] Use external monotonic elapsed time for throughput; preserve all samples and report medians/ranges, Get/Put P99 and CPU.
- [x] Separate diagnostic runs measure Submit→Apply and P→reader, plus notifier observation/notification lag; do not infer throughput gains from phase overlap.
- [x] Audit artifacts/hashes, document limits and regressions, and leave no load running.
