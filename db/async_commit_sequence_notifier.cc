// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include "db/async_commit_sequence_notifier.h"

#include "db/db_impl/db_impl.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
AsyncCommitSequenceNotifier::~AsyncCommitSequenceNotifier() { Stop(); }

void AsyncCommitSequenceNotifier::Start(SequenceNumber initial_sequence) {
  assert(!thread_.joinable());
  // Capture the recovered P before producers or readers can run. Starting the
  // thread later is harmless: its event epochs retain intervening signals.
  thread_ = std::thread([this, initial_sequence] { Run(initial_sequence); });
}

void AsyncCommitSequenceNotifier::Stop() {
  if (!thread_.joinable()) return;
  stop_.store(true, std::memory_order_release);
  waiter_event_.NotifyAll();
  progress_event_.NotifyAll();
  thread_.join();
}

void AsyncCommitSequenceNotifier::PublicationAdvanced() {
  // No CV waiters: the notifier sleeps on waiter_event_, so this only advances
  // an epoch and does not wake a thread or acquire a reader slot mutex.
  progress_event_.NotifyAll();
}

void AsyncCommitSequenceNotifier::RegisterWaiter() {
  if (waiters_.fetch_add(1) == 0) waiter_event_.NotifyAll();
}

void AsyncCommitSequenceNotifier::UnregisterWaiter() {
  const size_t previous = waiters_.fetch_sub(1);
  assert(previous != 0);
  // Let a notifier waiting for progress return to the idle event, including
  // when the last waiter timed out without any publication.
  if (previous == 1) progress_event_.NotifyAll();
}

void AsyncCommitSequenceNotifier::Run(SequenceNumber notified) {
  TEST_SYNC_POINT("AsyncCommitSequenceNotifier::Run:Started");
  while (!stop_.load(std::memory_order_acquire)) {
    if (waiters_.load() == 0) {
      // Do not skip P here. A first waiter can register and a worker can
      // publish after the zero-count observation; advancing N would lose its
      // interval.
      const uint32_t epoch = waiter_event_.PrepareWait();
      TEST_SYNC_POINT("AsyncCommitSequenceNotifier::Run:IdleRegistered");
      if (!stop_.load(std::memory_order_acquire) && waiters_.load() == 0) {
        TEST_SYNC_POINT_CALLBACK(
            "AsyncCommitSequenceNotifier::Run:BeforeIdleWait", this);
        waiter_event_.Wait(epoch);
      }
      waiter_event_.CancelWait();
      continue;
    }

    const SequenceNumber published = db_->versions_->LastSequence();
    if (published > notified) {
      db_->NotifyAsyncCommitPublication(notified, published);
      // Use this round's snapshot. Publications during slot notification are
      // covered next round, even if they map to the very same slot.
      notified = published;
      TEST_SYNC_POINT_CALLBACK("AsyncCommitSequenceNotifier::Run:Notified",
                               &notified);
      continue;
    }

    const uint32_t epoch = progress_event_.PrepareWait();
    TEST_SYNC_POINT("AsyncCommitSequenceNotifier::Run:ProgressRegistered");
    if (!stop_.load(std::memory_order_acquire) && waiters_.load() != 0 &&
        db_->versions_->LastSequence() <= notified) {
      TEST_SYNC_POINT_CALLBACK(
          "AsyncCommitSequenceNotifier::Run:BeforeProgressWait", this);
      progress_event_.Wait(epoch);
    }
    progress_event_.CancelWait();
  }
  TEST_SYNC_POINT("AsyncCommitSequenceNotifier::Run:Stopped");
}
}  // namespace ROCKSDB_NAMESPACE
