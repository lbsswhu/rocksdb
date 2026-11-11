# Single-worker batch acquisition and per-group publication

Base is the current uncommitted WriteGroup redesign, checkpoint HEAD 785a62dc6. The user supplied the execution order. Implement in the existing linked worktree. Preserve the prior changes and their benchmark artifacts.

Follow-up validation restored only sync=true to notify followers before releasing the WAL gate, retaining WakeOne and this serial batch protocol. The sync-aware handoff test covers both ordering policies. Current source matches the verified 874-test Debug and 723-test ASAN/UBSAN runs; matched performance results with pipeline off and on are archived under `build/async-restored-overall-perf-20261111/` and `build/async-pipeline-overall-perf-20261111/` (84 formal runs each).

- [x] Extend the serial integration test to queue 65 ready groups behind a blocked group, using fetch=1. At the last apply, assert that the previous prefix is readable and this whole batch is still charged. Pause the final resource release and assert that a drain is still pending even though P covers the batch. Observe the test fail against the base.
- [x] Separate serial Run: collect the ready FIFO prefix up to a snapshot of LastSubmitted without waiting for later submissions. Reuse owning and raw-pointer vectors; respect exclusive fences. Fetch configuration is for multiple workers.
- [x] Factor shared ApplyGroup. Publish each successful serial group under the error mutex and signal the notifier outside it. At batch end use the bulk resource release, retire D and quotas under one lock, and notify management once. Preserve multi-worker completion protocol and WAL handoff.
- [x] Verify a late group failure freezes P at the previous success while all acquired tasks are cleaned. Run the affected Debug and sanitizer suites, build Release, and check runtime artifacts. No benchmark without an advance reminder.
