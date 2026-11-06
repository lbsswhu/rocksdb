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

size_t AsyncCommitExecutor::TaskStorageBytes(size_t ranges, bool exclusive) {
  return (exclusive ? 1 : ranges) * sizeof(AsyncCommitTaskQueue::Node);
}

Status AsyncCommitExecutor::PrepareGroup(
    const std::shared_ptr<AsyncCommitGroup>& group) {
  if (!accepting_.load()) return Status::ShutdownInProgress();
  assert(group->merged_batch && !group->ranges.empty());
  const size_t count = group->exclusive ? 1 : group->ranges.size();
  for (size_t i = 0; i < count; ++i) {
    auto* node = new AsyncCommitTaskQueue::Node(AsyncCommitTask(group, i));
    if (group->last_task) group->last_task->next.store(node);
    else group->first_task = node;
    group->last_task = node;
  }
  group->remaining.store(count);
  group->previous_barrier = last_exclusive_;
  // This is before WAL, and may wait for an older completion. Publication after
  // a successful WAL never acquires this registry/publication mutex.
  std::lock_guard<std::mutex> lock(db_->async_commit_mutex_);
  group->group_id = ++prepared_;
  if (tail_) tail_->next = group;
  else head_ = group;
  tail_ = group;
  return Status::OK();
}

void AsyncCommitExecutor::CancelPreparedGroup(
    const std::shared_ptr<AsyncCommitGroup>& group) {
  auto* node = group->first_task;
  group->first_task = group->last_task = nullptr;
  while (node) {
    auto* next = node->next.load();
    delete node;
    node = next;
  }
  group->remaining.store(1);
  Complete(group);
}

void AsyncCommitExecutor::SubmitPreparedGroup(
    const std::shared_ptr<AsyncCommitGroup>& group) {
  const uint64_t start = db_->immutable_db_options_.clock->NowMicros();
  auto* first = group->first_task;
  auto* last = group->last_task;
  assert(first && last && group->group_id != 0);
  group->first_task = group->last_task = nullptr;
  group->queued_micros = start;
  group->queued = true;
  if (group->ranges.size() == 1)
    single_copy_bytes_.fetch_add(WriteBatchInternal::ByteSize(group->apply_batch),
                                 std::memory_order_relaxed);
  if (group->exclusive) last_exclusive_ = group->group_id;
  last_submitted_.store(group->group_id, std::memory_order_release);
  submitted_.fetch_add(1, std::memory_order_relaxed);
  queue_.PushChain(first, last);
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
  while (drained_ticket_ < group_id) {
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
  AsyncCommitTask pending;
  for (;;) {
    std::array<AsyncCommitTask, 64> refs;
    if (pending.group) refs[0] = std::move(pending);
    else if (!TakeOrWait(worker, &refs[0])) return;
    size_t count = 1;
    size_t bytes = refs[0].group->exclusive
        ? WriteBatchInternal::ByteSize(refs[0].group->apply_batch) - WriteBatchInternal::kHeader
        : refs[0].group->ranges[refs[0].index].end_offset -
          refs[0].group->ranges[refs[0].index].begin_offset;
    // Multiple workers take one range per lease to preserve prompt group-local
    // parallelism. The serial worker amortizes scheduling across queued groups.
    while (workers_ == 1 && !refs[0].group->exclusive && count < refs.size() &&
           bytes < 65536) {
      AsyncCommitTask next;
      if (!queue_.TryPop(worker, &next)) break;
      const auto& group = *next.group;
      if (group.exclusive || group.previous_barrier != refs[0].group->previous_barrier) {
        pending = std::move(next);
        break;
      }
      size_t size = group.ranges[next.index].end_offset - group.ranges[next.index].begin_offset;
      if (size > 65536 - bytes) {
        pending = std::move(next);
        break;
      }
      bytes += size;
      refs[count++] = std::move(next);
    }
    // Finish every previous local batch before waiting on its successor fence.
    WaitForFence(refs[0]);
    lease_batches_.fetch_add(1, std::memory_order_relaxed);
    leased_bytes_.fetch_add(bytes, std::memory_order_relaxed);
    const uint64_t apply_start = db_->immutable_db_options_.clock->NowMicros();
    uint64_t ranges = 0, queued = 0;
    for (size_t i = 0; i < count; ++i) {
      auto& ref = refs[i];
      auto& group = *ref.group;
      ranges += group.exclusive ? group.ranges.size() : 1;
      if (apply_start >= group.queued_micros) queued += apply_start - group.queued_micros;
      Status status = db_->AsyncCommitStatus(false);
      if (status.ok()) {
        TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:BeforeApply", group.apply_batch);
        status = db_->AsyncCommitStatus(false);
        if (status.ok()) {
          if (group.exclusive) {
            for (size_t r = 0; r < group.ranges.size() && status.ok(); ++r)
              status = db_->ApplyAsyncCommitRange(group, r, false);
          } else {
            status = db_->ApplyAsyncCommitRange(group, ref.index, workers_ > 1);
          }
        }
        TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:ApplyStatus", &status);
        if (!status.ok()) {
          db_->FailAsyncCommit(status);
          InstrumentedMutexLock db_lock(&db_->mutex_);
          db_->error_handler_.SetBGError(status, BackgroundErrorReason::kMemTable)
              .PermitUncheckedError();
        }
      }
    }
    leased_ranges_.fetch_add(ranges, std::memory_order_relaxed);
    queue_micros_.fetch_add(queued, std::memory_order_relaxed);
    {
      InstrumentedMutexLock db_lock(&db_->mutex_);
      AsyncCommitGroup* previous = nullptr;
      for (size_t i = 0; i < count; ++i) {
        if (refs[i].group.get() != previous) {
          previous = refs[i].group.get();
          db_->ScheduleAsyncCommitTargets(*previous);
        }
      }
    }
    const uint64_t apply_end = db_->immutable_db_options_.clock->NowMicros();
    if (apply_end >= apply_start)
      apply_micros_.fetch_add(apply_end - apply_start, std::memory_order_relaxed);
    for (size_t i = 0; i < count; ++i) Complete(refs[i].group);
  }
}

void AsyncCommitExecutor::Complete(const std::shared_ptr<AsyncCommitGroup>& group) {
  if (group->remaining.fetch_sub(1, std::memory_order_acq_rel) != 1) return;
  const size_t metadata = sizeof(AsyncCommitGroup) +
                          4 * sizeof(std::shared_ptr<AsyncCommitGroup>);
  const size_t old_bytes = group->reserved_bytes;
  db_->ReleaseAsyncCommitResources(group.get());
  {
    std::lock_guard<std::mutex> lock(db_->async_commit_mutex_);
    TEST_SYNC_POINT("AsyncCommitExecutor::Complete:Locked");
    assert(old_bytes >= metadata);
    db_->async_commit_pending_bytes_ -= old_bytes - metadata;
    group->reserved_bytes = metadata;
    group->complete = true;
    while (head_ && head_->complete) {
      auto retired = head_;
      head_ = std::move(retired->next);
      if (!head_) tail_.reset();
      if (retired->queued) {
        if (db_->async_commit_error_.ok()) {
          assert(retired->last_sequence >= db_->versions_->LastSequence());
          db_->versions_->SetLastSequence(retired->last_sequence);
        }
        drained_.fetch_add(1, std::memory_order_relaxed);
      }
      drained_ticket_ = retired->group_id;
      db_->async_commit_pending_bytes_ -= retired->reserved_bytes;
      retired->reserved_bytes = 0;
      --db_->async_commit_pending_groups_;
    }
  }
  db_->async_commit_cv_.notify_all();
}
}  // namespace ROCKSDB_NAMESPACE
