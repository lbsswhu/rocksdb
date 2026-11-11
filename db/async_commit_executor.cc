// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include "db/async_commit_executor.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <thread>

#include "db/async_commit_sequence_notifier.h"
#include "db/db_impl/db_impl.h"
#include "db/write_batch_internal.h"
#include "port/port.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
namespace {
constexpr uint64_t kAsyncCommitIdleMicros = 50;
}
AsyncCommitExecutor::AsyncCommitExecutor(DBImpl* db)
    : db_(db), workers_(db->immutable_db_options_.async_commit_memtable_threads),
      queue_(workers_) {}
AsyncCommitExecutor::~AsyncCommitExecutor() {
  if (!joined_) BeginShutdownAndDrain().PermitUncheckedError();
}

void AsyncCommitExecutor::Start() {
  idle_micros_ = kAsyncCommitIdleMicros;
  TEST_SYNC_POINT_CALLBACK("AsyncCommitExecutor::Start:IdleMicros", &idle_micros_);
  pool_.SetHostEnv(db_->env_);
  pool_.SetBackgroundThreads(static_cast<int>(workers_));
  for (size_t i = 0; i < workers_; ++i)
    pool_.SubmitJob([this, i] { Run(i); });
  std::unique_lock<std::mutex> lock(db_->async_commit_mutex_);
  db_->async_commit_cv_.wait(lock, [&] { return started_workers_ == workers_; });
}

size_t AsyncCommitExecutor::TaskStorageBytes() {
  return sizeof(AsyncCommitTaskQueue::Node);
}

Status AsyncCommitExecutor::PrepareGroup(
    const std::shared_ptr<AsyncCommitGroup>& group) {
  if (!accepting_.load()) return Status::ShutdownInProgress();
  assert((group->merged_batch || group->inline_batch) && group->apply_batch &&
         !group->ranges.empty());
  group->first_task = new AsyncCommitTaskQueue::Node(AsyncCommitTask(group));
  group->previous_barrier = last_exclusive_;
  // Prepared before WAL: no allocation or reference acquisition is needed
  // when a multi-worker task transfers its completion record to the notifier.
  if (workers_ > 1) group->completion_owner = group;
  return Status::OK();
}

void AsyncCommitExecutor::CancelPreparedGroup(
    const std::shared_ptr<AsyncCommitGroup>& group) {
  assert(!group->queued && group->group_id == 0);
  delete group->first_task;
  group->first_task = nullptr;
  group->completion_owner.reset();
  db_->ReleaseAsyncCommitGroup(group.get());
}

void AsyncCommitExecutor::SubmitPreparedGroup(
    const std::shared_ptr<AsyncCommitGroup>& group) {
  const uint64_t start = db_->immutable_db_options_.clock->NowMicros();
  auto* first = group->first_task;
  assert(first && group->group_id == 0);
  group->group_id = ++prepared_;
  group->first_task = nullptr;
  group->queued_micros = start;
  group->queued = true;
  if (group->ranges.size() == 1)
    single_copy_bytes_.fetch_add(WriteBatchInternal::ByteSize(group->apply_batch),
                                 std::memory_order_relaxed);
  if (group->exclusive) last_exclusive_ = group->group_id;
  last_submitted_.store(group->group_id, std::memory_order_release);
  submitted_.fetch_add(1, std::memory_order_relaxed);
  queue_.PushChain(first, first);
  // No access to payload/ranges/targets after publication: workers can finish
  // before the WAL caller even begins notifying its followers.
  const uint64_t end = db_->immutable_db_options_.clock->NowMicros();
  if (end >= start) submit_micros_.fetch_add(end - start, std::memory_order_relaxed);
}

uint64_t AsyncCommitExecutor::LastSubmitted() {
  return last_submitted_.load(std::memory_order_acquire);
}

Status AsyncCommitExecutor::DrainThrough(uint64_t group_id) {
  std::unique_lock<std::mutex> lock(db_->async_commit_mutex_);
  assert(group_id <= LastSubmitted());
  while (drained_ticket_ < group_id ||
         (db_->async_commit_error_.ok() && published_ticket_ < group_id)) {
    TEST_SYNC_POINT("AsyncCommitExecutor::DrainThrough:Wait");
    db_->async_commit_cv_.wait(lock);
  }
  return db_->async_commit_error_;
}

Status AsyncCommitExecutor::BeginShutdownAndDrain() {
  if (joined_) return db_->AsyncCommitStatus(false);
  accepting_.store(false);
  Status status = DrainThrough(LastSubmitted());
  stop_.store(true);
  WakeAll();
  pool_.WaitForJobsAndJoinAllThreads();
  joined_ = true;
  return status;
}

void AsyncCommitExecutor::WakeAll() {
  if (event_.NotifyAll()) wake_calls_.fetch_add(1, std::memory_order_relaxed);
}

void AsyncCommitExecutor::WakeOne() {
  if (event_.NotifyOne()) wake_calls_.fetch_add(1, std::memory_order_relaxed);
}

bool AsyncCommitExecutor::GetIntProperty(const Slice& name, uint64_t* value) {
  if (name == "rocksdb.async-commit-executor-submitted-groups")
    *value = submitted_.load();
  else if (name == "rocksdb.async-commit-executor-drained-groups")
    *value = drained_.load();
  else if (name == "rocksdb.async-commit-executor-lease-batches")
    *value = lease_batches_.load();
  else if (name == "rocksdb.async-commit-executor-lease-ranges")
    *value = leased_ranges_.load();
  else if (name == "rocksdb.async-commit-executor-lease-bytes")
    *value = leased_bytes_.load();
  else if (name == "rocksdb.async-commit-executor-single-copy-bytes")
    *value = single_copy_bytes_.load();
  else if (name == "rocksdb.async-commit-executor-queue-micros")
    *value = queue_micros_.load();
  else if (name == "rocksdb.async-commit-executor-apply-micros")
    *value = apply_micros_.load();
  else if (name == "rocksdb.async-commit-executor-idle-polls")
    *value = idle_polls_.load();
  else if (name == "rocksdb.async-commit-executor-sleep-calls")
    *value = sleep_calls_.load();
  else if (name == "rocksdb.async-commit-executor-wake-calls")
    *value = wake_calls_.load();
  else if (name == "rocksdb.async-commit-executor-submit-micros")
    *value = submit_micros_.load();
  else if (name == "rocksdb.async-commit-executor-queue-cas-retries")
    *value = queue_.CASRetries();
  else if (name == "rocksdb.async-commit-executor-retired-nodes")
    *value = queue_.RetiredCount();
  else if (name == "rocksdb.async-commit-executor-idle-micros")
    *value = idle_micros_;
  else return false;
  return true;
}

bool AsyncCommitExecutor::TakeOrWait(size_t worker, AsyncCommitTask* task) {
  auto empty_since = std::chrono::steady_clock::now();
  for (;;) {
    if (queue_.TryPop(worker, task)) return true;
    if (stop_.load()) return false;
    TEST_SYNC_POINT("AsyncCommitExecutor::Run:IdlePoll");
    idle_polls_.fetch_add(1, std::memory_order_relaxed);
    const auto idle = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - empty_since).count();
    if (idle < 10 && static_cast<uint64_t>(idle) < idle_micros_) {
      port::AsmVolatilePause();
      continue;
    }
    if (static_cast<uint64_t>(idle) < idle_micros_) {
      std::this_thread::yield();
      continue;
    }
    queue_.Reclaim(worker);
    const auto epoch = event_.PrepareWait();
    if (queue_.TryPop(worker, task)) {
      event_.CancelWait();
      return true;
    }
    if (stop_.load()) {
      event_.CancelWait();
      return false;
    }
    TEST_SYNC_POINT("AsyncCommitExecutor::Run:BeforeSleep");
    sleep_calls_.fetch_add(1, std::memory_order_relaxed);
    event_.Wait(epoch);
    event_.CancelWait();
    empty_since = std::chrono::steady_clock::now();
  }
}

void AsyncCommitExecutor::WaitForFence(const AsyncCommitTask& task) {
  const auto& group = *task.group;
  uint64_t target = group.exclusive ? group.group_id - 1
                                    : group.previous_barrier;
  if (target == 0) return;
  std::unique_lock<std::mutex> lock(db_->async_commit_mutex_);
  while (drained_ticket_ < target) {
    TEST_SYNC_POINT_CALLBACK("AsyncCommitExecutor::Run:ExclusiveWait", &target);
    db_->async_commit_cv_.wait(lock);
  }
}

void AsyncCommitExecutor::Run(size_t worker) {
  {
    std::lock_guard<std::mutex> lock(db_->async_commit_mutex_);
    ++started_workers_;
    db_->async_commit_cv_.notify_all();
  }
  if (workers_ == 1) {
    RunSerial(worker);
    return;
  }
  const size_t fetch_limit =
      db_->immutable_db_options_.async_commit_memtable_groups_per_fetch;
  AsyncCommitTask pending;
  for (;;) {
    std::array<AsyncCommitTask, 64> refs;
    if (pending.group) refs[0] = std::move(pending);
    else if (!TakeOrWait(worker, &refs[0]))
      return;
    size_t count = 1;
    while (!refs[0].group->exclusive && count < fetch_limit) {
      AsyncCommitTask next;
      if (!queue_.TryPop(worker, &next)) break;
      if (next.group->exclusive ||
          next.group->previous_barrier != refs[0].group->previous_barrier) {
        pending = std::move(next);
        break;
      }
      refs[count++] = std::move(next);
    }
    lease_batches_.fetch_add(1, std::memory_order_relaxed);
    for (size_t i = 0; i < count; ++i) {
      auto& group = *refs[i].group;
      WaitForFence(refs[i]);
      ApplyGroup(group);
      // Fetching multiple groups never delays an earlier group's completion.
      Complete(refs[i].group);
    }
  }
}

void AsyncCommitExecutor::RunSerial(size_t worker) {
  AsyncCommitTask pending;
  // Keep task ownership until cleanup. Reuse the arrays across leases.
  std::vector<AsyncCommitTask> tasks;
  std::vector<AsyncCommitGroup*> groups;
  for (;;) {
    AsyncCommitTask first;
    if (pending.group)
      first = std::move(pending);
    else if (!TakeOrWait(worker, &first))
      return;

    // Bound this lease by the producer's current submission ticket. A producer
    // can reserve a ticket just before linking its node; never wait for it.
    const uint64_t ready_through = LastSubmitted();
    tasks.push_back(std::move(first));
    groups.push_back(tasks.back().group.get());
    while (!groups.front()->exclusive &&
           groups.back()->group_id < ready_through) {
      AsyncCommitTask next;
      if (!queue_.TryPop(worker, &next)) break;
      if (next.group->exclusive ||
          next.group->previous_barrier != groups.front()->previous_barrier) {
        pending = std::move(next);
        break;
      }
      tasks.push_back(std::move(next));
      groups.push_back(tasks.back().group.get());
    }
    lease_batches_.fetch_add(1, std::memory_order_relaxed);
    WaitForFence(tasks.front());

    for (auto* group : groups) {
      ApplyGroup(*group);
      SequenceNumber previous, published;
      {
        std::lock_guard<std::mutex> lock(db_->async_commit_mutex_);
        TEST_SYNC_POINT("AsyncCommitExecutor::Complete:Locked");
        previous = db_->versions_->LastSequence();
        PublishCompletedGroup(*group);
        published = db_->versions_->LastSequence();
      }
      db_->SignalAsyncCommitPublication(previous, published);
    }

    TEST_SYNC_POINT("AsyncCommitExecutor::RunSerial:BeforeRelease");
    db_->ReleaseAsyncCommitResources(groups.data(), groups.size());
    {
      std::lock_guard<std::mutex> lock(db_->async_commit_mutex_);
      RetireCleanedGroups(groups.data(), groups.size());
    }
    db_->async_commit_cv_.notify_all();
    groups.clear();
    tasks.clear();
  }
}

void AsyncCommitExecutor::ApplyGroup(AsyncCommitGroup& group) {
  const uint64_t apply_start = db_->immutable_db_options_.clock->NowMicros();
  const size_t ranges = group.ranges.size();
  leased_ranges_.fetch_add(ranges, std::memory_order_relaxed);
  leased_bytes_.fetch_add(WriteBatchInternal::ByteSize(group.apply_batch) -
                              WriteBatchInternal::kHeader,
                          std::memory_order_relaxed);
  if (apply_start >= group.queued_micros)
    queue_micros_.fetch_add((apply_start - group.queued_micros) * ranges,
                            std::memory_order_relaxed);
  Status status;
  if (db_->async_commit_wait_stopped_.load(std::memory_order_acquire))
    status = db_->AsyncCommitStatus(false);
  if (status.ok()) {
    TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:BeforeApply",
                             group.apply_batch);
    if (db_->async_commit_wait_stopped_.load(std::memory_order_acquire))
      status = db_->AsyncCommitStatus(false);
    if (status.ok()) {
      for (size_t r = 0; r < ranges && status.ok(); ++r)
        status = db_->ApplyAsyncCommitRange(group, r,
                                            workers_ > 1 && !group.exclusive);
    }
    TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:ApplyStatus", &status);
    if (!status.ok()) {
      db_->FailAsyncCommit(status);
      InstrumentedMutexLock db_lock(&db_->mutex_);
      db_->error_handler_.SetBGError(status, BackgroundErrorReason::kMemTable)
          .PermitUncheckedError();
    }
  }
  const uint64_t apply_end = db_->immutable_db_options_.clock->NowMicros();
  if (apply_end >= apply_start)
    apply_micros_.fetch_add(apply_end - apply_start, std::memory_order_relaxed);
}

void AsyncCommitExecutor::Complete(
    const std::shared_ptr<AsyncCommitGroup>& group) {
  assert(workers_ > 1);
  db_->ReleaseAsyncCommitResources(group.get());
  db_->async_commit_sequence_notifier_->SubmitCompleted(group);
}

void AsyncCommitExecutor::PublishCompletedGroup(const AsyncCommitGroup& group) {
  if (!db_->async_commit_error_.ok()) return;
  assert(group.queued && group.group_id == published_ticket_ + 1);
  assert(group.last_sequence >= db_->versions_->LastSequence());
  db_->versions_->SetLastSequence(group.last_sequence);
  published_ticket_ = group.group_id;
}

void AsyncCommitExecutor::RetireCleanedGroups(AsyncCommitGroup* const* groups,
                                              size_t count) {
  size_t reserved = 0;
  uint64_t ticket = drained_ticket_;
  for (size_t i = 0; i < count; ++i) {
    auto* group = groups[i];
    assert(group->queued && group->group_id == ticket + 1);
    ticket = group->group_id;
    reserved += group->reserved_bytes;
    group->reserved_bytes = 0;
  }
  assert(db_->async_commit_pending_bytes_ >= reserved);
  assert(db_->async_commit_pending_groups_ >= count);
  drained_ticket_ = ticket;
  drained_.fetch_add(count, std::memory_order_relaxed);
  db_->async_commit_pending_bytes_ -= reserved;
  db_->async_commit_pending_groups_ -= count;
}

void AsyncCommitExecutor::RetireCompletedGroup(AsyncCommitGroup* group) {
  assert(group->queued && group->group_id == drained_ticket_ + 1);
  PublishCompletedGroup(*group);
  RetireCleanedGroups(&group, 1);
}
}  // namespace ROCKSDB_NAMESPACE
