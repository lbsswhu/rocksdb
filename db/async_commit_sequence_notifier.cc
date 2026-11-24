// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include "db/async_commit_sequence_notifier.h"

#include <array>

#include "db/async_commit_executor.h"
#include "db/db_impl/db_impl.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
AsyncCommitSequenceNotifier::AsyncCommitSequenceNotifier(DBImpl* db)
    : db_(db),
      multi_worker_(db->immutable_db_options_.async_memtable_write_threads >
                    1) {}

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

void AsyncCommitSequenceNotifier::SubmitCompleted(
    const std::shared_ptr<AsyncCommitGroup>& group) {
  assert(multi_worker_ && group->completion_owner.get() == group.get());
  auto* head = completed_reports_.load(std::memory_order_relaxed);
  do {
    group->completion_next = head;
  } while (!completed_reports_.compare_exchange_weak(
      head, group.get(), std::memory_order_release, std::memory_order_relaxed));
  // The notifier owns this record after publication. Do not touch it again.
  progress_event_.NotifyAll();
  TEST_SYNC_POINT("AsyncCommitSequenceNotifier::SubmitCompleted:Reported");
}

bool AsyncCommitSequenceNotifier::HasCompleted() const {
  return pending_reports_ != nullptr ||
         completed_reports_.load(std::memory_order_acquire) != nullptr;
}

void AsyncCommitSequenceNotifier::ProcessCompleted() {
  if (!pending_reports_) {
    auto* reports =
        completed_reports_.exchange(nullptr, std::memory_order_acquire);
    // The MPSC inbox pushes at its head. Restore completion arrival order so
    // an already-ready prefix is not buried beneath newer groups for several
    // bounded notification turns. These detached links are notifier-owned.
    while (reports) {
      auto* next = reports->completion_next;
      reports->completion_next = pending_reports_;
      pending_reports_ = reports;
      reports = next;
    }
  }
  if (!pending_reports_) return;
  std::array<AsyncCommitGroup*, 64> incoming;
  size_t count = 0;
  while (pending_reports_ && count < incoming.size()) {
    auto* raw = pending_reports_;
    pending_reports_ = raw->completion_next;
    auto group = std::move(raw->completion_owner);
    assert(group.get() == raw && group->queued);
    incoming[count++] = raw;
    // Only this thread mutates next. Keep metadata and its quota while a
    // predecessor is missing, even though payload and pinned refs are gone.
    auto* position = &completed_;
    while (*position && (*position)->group_id < group->group_id)
      position = &(*position)->next;
    assert(!*position || (*position)->group_id != group->group_id);
    group->next = std::move(*position);
    *position = std::move(group);
    TEST_SYNC_POINT_CALLBACK(
        "AsyncCommitSequenceNotifier::ProcessCompleted:Received",
        &raw->group_id);
  }
  SequenceNumber previous, published;
  {
    std::lock_guard<std::mutex> lock(db_->async_commit_mutex_);
    TEST_SYNC_POINT("AsyncCommitExecutor::Complete:Locked");
    previous = db_->versions_->LastSequence();
    for (size_t i = 0; i < count; ++i) {
      auto* group = incoming[i];
      const size_t metadata = group->MetadataBytes();
      assert(group->reserved_bytes >= metadata);
      db_->async_commit_pending_bytes_ -= group->reserved_bytes - metadata;
      group->reserved_bytes = metadata;
    }
    auto* executor = db_->async_commit_executor_.get();
    while (completed_ &&
           completed_->group_id == executor->drained_ticket_ + 1) {
      auto retired = std::move(completed_);
      completed_ = std::move(retired->next);
      executor->RetireCompletedGroup(retired.get());
    }
    published = db_->versions_->LastSequence();
  }
  db_->async_commit_cv_.notify_all();
  db_->SignalAsyncCommitPublication(previous, published);
}

void AsyncCommitSequenceNotifier::RegisterWaiter() {
  if (waiters_.fetch_add(1) == 0) {
    if (multi_worker_)
      progress_event_.NotifyAll();
    else
      waiter_event_.NotifyAll();
  }
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
    if (multi_worker_) ProcessCompleted();
    if (waiters_.load() == 0) {
      // Do not skip P here. A first waiter can register and a worker can
      // publish after the zero-count observation; advancing N would lose its
      // interval.
      // Multiple workers still need prefix/accounting progress with no readers.
      auto& event = multi_worker_ ? progress_event_ : waiter_event_;
      const uint32_t epoch = event.PrepareWait();
      TEST_SYNC_POINT("AsyncCommitSequenceNotifier::Run:IdleRegistered");
      if (!stop_.load(std::memory_order_acquire) && waiters_.load() == 0 &&
          (!multi_worker_ || !HasCompleted())) {
        TEST_SYNC_POINT_CALLBACK(
            "AsyncCommitSequenceNotifier::Run:BeforeIdleWait", this);
        event.Wait(epoch);
      }
      event.CancelWait();
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
        (!multi_worker_ || !HasCompleted()) &&
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
