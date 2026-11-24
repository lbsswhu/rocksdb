# Async commit implementation plan

> Implement inline with superpowers:executing-plans, using test-driven-development.

**Goal:** Implement the native first version of async commit described in `docs/design/async_commit.md` on RocksDB 6.26.1.

**Architecture:** Original WAL leaders own and apply each group. Followers leave only after WAL success and complete queue detachment. One representative per original group preserves ordered memtable application with pipelined WAL overlap. DB-owned thread-local sequence dependencies gate new user read views.

**Tech stack:** C++11, RocksDB WriteThread, ThreadLocalPtr, condition variables, gtest and SyncPoint; CMake debug build.

**Spec:** `docs/design/async_commit.md` (existing user-approved design).

## Global constraints

- `async_memtable_write=true` by default (updated 2026-11-13); explicitly disable it for the ordinary write path.
- `max_async_memtable_write_pending_bytes=67108864`, immutable and nonzero.
- WAL and memtable reuse one group buffer; single-writer groups borrow the caller batch.
- Never access follower Writer/batch after notification. One original leader applies each group serially.
- Publish only complete groups, never advance past a terminal error. Read waiting precedes SuperVersion acquisition.
- R is native-thread and DB-instance specific; explicit historical snapshots retain their sequence.
- Java/JNI integration is a subsequent binding task per the design; native db_bench integration is in scope.

## Review focus

- Concurrent enqueue/write stall during follower removal must leave no dangling pointers.
- Group failure after partial insertion must stop later publication and unblock dependency reads and barriers.
- CF handle destruction/drop and memtable switching must respect all pending representatives.
- Empty/log-only batches, ignored missing CFs and integrity protection must consume exactly their range's sequences/checksums.
- Capacity waits must not hold DB/queue-state locks or block earlier leaders from releasing memory.

## Task 1: Native options and scope validation

Files: `include/rocksdb/options.h`, `options/db_options.*`, `options/options_helper.cc`, `db/db_impl/db_impl_open.cc`, `db/column_family.cc`, transaction wrappers, `db/async_commit_test.cc`, build registration.

- [x] Add a parser/open/round-trip test; run it and record the expected unknown-option failure.
- [x] Add immutable options, logging and reconstruction; reject unsupported DB, CF and transaction configurations, including CF creation/SetOptions.
- [x] Verify parser, persisted options and rejection tests.

## Task 2: Stable group data and queue transfer

Files: `db/write_thread.*`, `db/write_batch_internal.h`, `db/write_batch.cc`, `db/db_impl/db_impl.h`, new `db/db_impl/db_impl_async_commit.cc`.

- [x] Add deterministic early-follower/owned-buffer/order tests before implementation.
- [x] Implement byte-range validation/insertion and checksum offset handling without sub-batch copies.
- [x] Implement `DetachAsyncFollowersKeepLeader`, `TransferAsyncGroupToMemtable`, `WaitForAsyncMemtableWriter`, single-representative completion and follower WAL state.
- [x] Implement validation, bounded reservation, one merge, prepared WAL writing, original-leader serial apply, ordered publication and terminal cleanup.
- [x] Verify both pipelined modes, stack reuse, missing-CF options, log-only and protected batches.

## Task 3: Thread dependencies and user read views

Files: `db/db_impl/db_impl.h`, `db/db_impl/db_impl.cc`, `db/db_impl/db_impl_write.cc`, `db/arena_wrapped_db_iter.cc`, async implementation.

- [x] Add write/read blocking, timeout/retry, historical snapshot, refresh, cross-DB and failure tests.
- [x] Allocate thread context before enqueue; successful returns update R to the batch end.
- [x] Implement `PrepareAsyncCommitRead(const ReadOptions&)`, deadline-aware condition waiting, terminal error and close notification.
- [x] Gate public Get/GetMergeOperands, all MultiGet overloads, iterators, snapshots, KeyMayExist and Refresh. Reject tailing reads. Keep internal GetImpl/snapshot helpers free of unconditional waits.
- [x] Verify all read status contracts and atomic view publication.

## Task 4: Operational integration and correctness coverage

Files: `tools/db_bench_tool.cc`, async implementation, `db/async_commit_test.cc`, options regression tests.

- [x] Wire both db_bench flags into Options; preserve original request counts and existing WAL/memtable timers.
- [x] Test all async/pipelined/concurrent/sync combinations, capacity/no_slowdown, faults, CF lifecycle, flush/recovery and threaded stress.
- [x] Build and run async_commit_test plus db_write_test, db_options_test, options_settable_test, options_file_test, write_callback_test and related regressions.
- [x] Verify native benchmark flags and persisted effective options with a small pure-write smoke run; performance claims require separate measured runs.

## Task 5: Review and final verification

- [x] Review the full diff against I1-I11 and the review focus; fix correctness issues with reproducing tests.
- [x] Run sanitizer checks where feasible and report exact tests, any failures and remaining limits.
- [x] Leave changes reviewable in this worktree; do not merge or publish.


## Implementation and validation notes

Native code lives in `db/db_impl/db_impl_async_commit.cc`; the original ordinary,
pipelined and unordered dispatches remain in `db_impl_write.cc`. Native options,
DB option reconstruction, log/OPTIONS output, the benchmark CLI, all user read
views and Refresh are connected. Async groups use original leaders, a separate
memtable representative, one owned WAL payload (or a borrowed singleton),
validated ranges and per-request missing-CF/hint options. The byte budget counts
allocation capacities and metadata; snapshots retain the existing P semantics.

Independent review found two correctness issues, both reproduced before fixing:
the explicit OptimisticTransactionDBOptions overload bypassed rejection, and an
empty DeleteRange could consume different sequences on replay after changing the
async option. Transaction validation now lives in the innermost Open overload.
Empty DeleteRange consumes its ordinary-record sequence in every mode, including
recovery, preserving WAL header counts and preventing sequence rollback/reuse.
This deliberately fixes a preexisting degenerate-range accounting bug outside
the flag as well; a mode-specific fix would be incompatible with unchanged WAL.

### Invariant coverage

| Invariants | Tests |
| --- | --- |
| I1, I2, I11 | FollowerOwnershipAndReadViews, ProtectedRangesMissingCFAndLogData, DetachedFollowerReentryAndWriteStall |
| I3, I4 | PipelinedOriginalGroupsOverlapInOrder, EmptyWALQueueConcurrentEnqueue, ReleasedCFHandleAndDropDrainPendingGroup |
| I5, I6 | AtomicCrossCFViewsAndIteratorRefresh, PartialInsertionFailureDrainsLaterGroupsAndBarrier, ApplicationFailureStopsPublicationAndReads |
| I7, I8 | FollowerOwnershipAndReadViews, DBAndThreadLifetimeIsolation, AtomicCrossCFViewsAndIteratorRefresh |
| I9, I10 | PendingCapacityAndNoSlowdown, WALFailureAcknowledgesNobodyAndDrains, PartialInsertionFailureDrainsLaterGroupsAndBarrier |
| Recovery | AcknowledgedWALRecoversAfterProcessExit, EmptyRangeSequenceSurvivesOptionChanges, ThreadedWritesFlushAndRecovery |

The parameter bits are pipelined=1, concurrent_memtable=2, sync=4. Tests for
pipelined-only interleavings exercise the odd parameter values; their even values
return because there is no independent memtable queue in that mode.

### Reproduce

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS_DEBUG='-O0 -g1' -DWITH_TESTS=ON -DWITH_GFLAGS=ON -DFAIL_ON_WARNINGS=OFF
cmake --build build --target async_commit_test db_bench -j3
./build/async_commit_test --gtest_color=no

cmake -S . -B build/asan -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS_DEBUG='-O0 -g1' -DWITH_TESTS=ON -DWITH_ASAN=ON -DWITH_UBSAN=ON -DFAIL_ON_WARNINGS=OFF
cmake --build build/asan --target async_commit_test -j3
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 ./build/asan/async_commit_test --gtest_color=no
```

Both runs passed all 141 registered cases, including real WAL append/sync
failures, terminal-error cleanup, CF lifetime, write stall, partial cross-CF
views, deadline/Refresh behavior and actual child-process termination before
application. The native db_bench smoke runs explicitly cover pipelined=false/true
and sync=false/true with 8 threads; each ends with pending_groups=0,
pending_bytes=0 and terminal_error=0, and effective OPTIONS fields are checked.
These Debug smoke runs do not establish throughput improvement.

Java/JNI integration remains deferred per the design's binding phase. One minor
review finding remains: range validation currently reparses each merged prefix;
this can cause quadratic validation work as the number of original requests per
group grows. It preserves correctness but needs measurement and optimization
before performance claims.


### Thread sanitizer and final regression results

```bash
cmake -S . -B build/tsan -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS_DEBUG='-O0 -g1' -DWITH_TESTS=ON -DWITH_TSAN=ON -DFAIL_ON_WARNINGS=OFF
setarch x86_64 -R cmake --build build/tsan --target async_commit_test -j3
setarch x86_64 -R env TSAN_OPTIONS=halt_on_error=1 ./build/tsan/async_commit_test --gtest_color=no
```

TSan passed 141/141 cases with no reports. ASLR is disabled for the build's gtest
discovery process and the test process because this host's GCC 11 sanitizer can
otherwise fail at startup with `unexpected memory mapping`. A separate minimal
condition-variable program also reproduced GCC 11's unsupported steady-clock
wait instrumentation; the new timed waits use the system-clock overload while
preserving the configured clock's remaining deadline duration. Tests cover
already-expired deadlines, expiration during waiting, and successful publication
before a future deadline. No race suppressions are used.

Existing regression programs passed 476/476 cases in total:

| Program | Passed |
| --- | ---: |
| db_write_test | 27 |
| db_options_test | 30 |
| options_settable_test | 3 |
| options_file_test | 2 |
| write_callback_test | 129 |
| db_memtable_test | 4 |
| db_sst_test | 25 |
| optimistic_transaction_test | 42 |
| write_batch_test | 18 |
| db_range_del_test | 42 |
| db_iterator_test | 113 |
| external_sst_file_basic_test | 41 |
