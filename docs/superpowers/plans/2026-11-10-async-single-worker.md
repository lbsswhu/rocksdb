# Async commit single-worker optimization

The user requested optimization of the existing single-worker implementation in
this order: publish completed work sooner, reduce WAL completion handoff costs,
then reduce Build/Prepare allocation and descriptor costs. Implement inline in
the existing worktree, with separate measurements for each change. Preserve
the pre-existing uncommitted notifier and reader-spin work.

## Contract and scope

- Preserve fixed reader entry A and continuous release/acquire publication P.
- Keep reader wait defaults 30/6/50, the separate notifier, WAL sync semantics,
  ordinary writer behavior, and multi-worker scheduling policies.
- Resource references must be released before drain tickets and quota are
  retired. Error, close, memtable switch, CF drop and exclusive Merge fences
  must retain their existing guarantees.
- Own all pool-mode payloads independently of the caller's batch and Writer.
- No public option changes or new tuning flags in this iteration.

## Step A: bounded completion chunks

Keep the existing lease of at most 64 ranges / 64 KiB, but apply and complete
single-worker work in chunks of eight ranges. Publish between chunks instead
of waiting for the entire lease. Eight is a candidate to evaluate against the
baseline; it is a work count, not a time budget.

For a chunk, collect groups whose final range finished. Release their batch
buffers outside the DB mutex, release their pinned CF/memtable references under
one DB mutex acquisition, and update completion/quota/drain under one async
mutex acquisition. Send one management notification and one notifier progress
event for that completion operation. Keep multi-worker completion to one range
per operation. Preserve failure freezing of P and cleanup of every group.

The queue latency counter will use each chunk's actual apply start, rather
than the original lease start; report this scope change explicitly. Apply time
continues to exclude Complete. Compare latency using independent endpoint
measurements and externally timed throughput, not changed counters alone.

## Step B: batch WAL state notifications

Initial candidate, single-worker pool only: use one queue-owned epoch event
for the blocking part of async WAL Writer waits. Each Writer still has its own
release-published state/status and acquire predicate. Initial/adaptive WAL
spinning remains unchanged. Writers register epoch, recheck their own state,
then wait; they do not enter STATE_LOCKED_WAITING in this path.

At async group completion, detach the old group safely, publish all final
states and the next leader state, and issue one event notification for the
group. Other state transitions notify immediately. This is a shared queue
event, not a private per-group event: later groups may observe harmless wakeups
and must recheck their own predicates. Measure this cost rather than assuming
it wins. Ordinary writers and unbatched management operations retain their
existing state mutex/CV path. Never access a Writer after its final state is
published; capture its predecessor/flags first.

The unfiltered shared-event candidate regressed and is retained only as an
experimental control. The next candidate keeps one epoch but assigns a TLS
thread mask, waits with FUTEX_WAIT_BITSET_PRIVATE and combines the old group
member masks into one FUTEX_WAKE_BITSET_PRIVATE notification. Next-leader
notification remains immediate and separate. Portable fallback may wake extra
callers but preserves the predicate/epoch protocol. Kernel checks confirmed
filtering across unrelated epoch advances; a wide-wake mutant was detected.

## Step C: reduce owned group preparation allocations

For the single worker, represent a whole group with one queue node instead of
one node per caller range. Retain all range metadata and apply every range with
its existing sequence, flags, validation and protection checks. Publish only
after the whole group completes. A multi-worker group retains per-range nodes.

For the single worker, use a derived group with an embedded owned WriteBatch
object to coallocate the descriptor and batch object. Other worker modes keep
the base group and existing separate batch object. The payload buffer remains separately owned and copied. Release
its capacity before retirement, and adjust reserved-byte accounting so the
embedded object is not charged twice. Keep the no-pool borrowed-batch path.
Do not add an unaccounted buffer/node cache or remove validation in this step.

The single-worker scheduling check is fused into the DB mutex already needed
for reference retirement, before any drain ticket/quota retirement. Rejected,
never-queued groups skip this check. Serial apply-micros consequently excludes
Schedule; multi-worker timing retains its previous scope. Whole groups retain
atomic completion, so the range target is a soft bound for an oversized group.

## Implementation and verification checklist

- [x] Archive baseline source patch, hashes, executable and shared library.
- [x] Add a deterministic regression showing early groups publish while a
      later range in the same lease is blocked; demonstrate baseline failure.
- [x] Implement Step A and run focused then existing executor tests.
- [x] Archive candidate A and compare baseline/candidate weak and strong mixed
      workloads; include pure writes as a throughput control.
- [x] Add deterministic epoch registration/publication races, single batched
      notification, blocked WAL failure, next-leader and lifetime tests.
- [x] Implement Step B and check existing write-thread/async tests.
- [x] Archive candidate AB and measure the same endpoints; retain only a
      supported improvement.
- [x] Test whole-group task ordering, quota, protected mixed-CF/empty ranges,
      Merge fencing, caller batch reuse, and post-error cleanup.
- [x] Implement Step C, run regression suites and sanitizer coverage.
- [x] Archive candidate ABC and compare against A/AB and baseline.
- [x] Final randomized serial comparison: independent native, current async
      disabled, original async, retained optimized weak and strong versions.
      Mixed sync=false/true, pure writes, and focused read-only/multi-worker
      regression controls. No builds or tests overlap benchmarks.
- [x] Verify effective OPTIONS, actual executable/library maps, all operation
      counts, hit/value sizes, WAL synchronization counts and final drain.
- [x] Report repeat ranges, Get/Put means/P99, CPU, material tradeoffs, and
      product delta; never mix probe and formal populations or add P99s.

Use three randomized serial repeats for performance comparisons, independent
clones of the existing 1M-key seed, same-process scan warmup, 16 foreground
threads, 1 KiB values, 50 Gets then 50 independent random overwrite Puts,
pipeline disabled, concurrent memtable disabled, one worker, and fixed 30/6/50.
Use CLOCK_MONOTONIC and completed operations including drain. Do not describe
the current binary with async disabled as the independent native version.

## Selection and validation status

The one-group completion candidate was tested separately; it reduced weak Put
P99 but lost weak throughput, so retain target eight ranges with whole groups.
Both variants passed Debug868 and ASAN/UBSAN675; the selected rebuilt binaries
match validated C8 archives byte for byte. Independent held-out mixed sync0
confirmation found weak throughput+10.4%, Get P99-7.5%, Put P99-4.5%; strong
throughput+0.7% does not establish a stable improvement. Native/off controls and
remaining workload guards are recorded in the artifact directory.

Final independent confirmation and all guards completed:150 valid performance
runs, including12 short preflights. Audit passed operation counts, hits/value
sizes, native/current-off separation, runtime mappings, defaults, WAL sync,
source/binary/seed hashes and complete drain. Weak pure-write median-4.8%,
read-only-5.0%, two-worker-3.3% have overlapping ranges; regression risk remains
and is disclosed. Strong pure-write+16.2% has disjoint ranges. No unconditional
performance gain is claimed. All performance collection has ended and the user
has been told other loads may resume. Full report is in the artifact directory.
