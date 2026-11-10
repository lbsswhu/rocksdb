// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <chrono>
#include <limits>
#include <thread>

#include "db/async_commit_executor.h"
#include "db/async_commit_sequence_notifier.h"
#include "db/db_impl/db_impl.h"
#include "db/write_batch_internal.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "test_util/sync_point.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {
class AsyncBatchValidator : public WriteBatch::Handler {
 public:
  AsyncBatchValidator(ColumnFamilySet* cfs,
                      std::vector<ColumnFamilyData*>* refs, bool ignore)
      : cfs_(cfs), refs_(refs), ignore_(ignore) {}
  Status PutCF(uint32_t cf, const Slice&, const Slice&) override {
    return CheckCF(cf);
  }
  Status DeleteCF(uint32_t cf, const Slice&) override { return CheckCF(cf); }
  Status SingleDeleteCF(uint32_t cf, const Slice&) override {
    return CheckCF(cf);
  }
  Status MergeCF(uint32_t cf, const Slice&, const Slice&) override {
    ColumnFamilyData* cfd = nullptr;
    Status status = CheckCF(cf, &cfd);
    if (status.ok() && cfd && !cfd->ioptions()->merge_operator) {
      return Status::InvalidArgument("async Merge requires a merge operator");
    }
    return status;
  }
  Status DeleteRangeCF(uint32_t cf, const Slice& begin,
                       const Slice& end) override {
    ColumnFamilyData* cfd = nullptr;
    Status status = CheckCF(cf, &cfd);
    if (status.ok() && cfd) {
      if (!cfd->is_delete_range_supported()) {
        return Status::NotSupported("DeleteRange is unsupported by this CF");
      }
      if (cfd->user_comparator()->Compare(begin, end) > 0) {
        return Status::InvalidArgument("end key comes before start key");
      }
    }
    return status;
  }

 private:
  Status CheckCF(uint32_t id, ColumnFamilyData** result = nullptr) {
    ColumnFamilyData* cfd = cfs_->GetColumnFamily(id);
    if (!cfd || cfd->IsDropped()) {
      return ignore_ ? Status::OK()
                     : Status::InvalidArgument(
                           "invalid async commit column family");
    }
    if (result) *result = cfd;
    if (std::find(refs_->begin(), refs_->end(), cfd) == refs_->end()) {
      cfd->Ref();
      refs_->push_back(cfd);
    }
    return Status::OK();
  }
  ColumnFamilySet* cfs_;
  std::vector<ColumnFamilyData*>* refs_;
  bool ignore_;
};
}  // namespace

Status DBImpl::AsyncCommitStatus(bool check_closing) {
  if (!immutable_db_options_.enable_async_commit) return Status::OK();
  std::lock_guard<std::mutex> lock(async_commit_mutex_);
  if (!async_commit_error_.ok()) return async_commit_error_;
  if (check_closing && async_commit_closing_)
    return Status::ShutdownInProgress();
  return Status::OK();
}

void DBImpl::FailAsyncCommit(const Status& status) {
  if (!immutable_db_options_.enable_async_commit || status.ok()) return;
  {
    std::lock_guard<std::mutex> lock(async_commit_mutex_);
    if (async_commit_error_.ok()) async_commit_error_ = status;
    async_commit_wait_stopped_.store(true, std::memory_order_release);
  }
  async_commit_cv_.notify_all();
  NotifyAllAsyncCommitWaiters();
  if (async_commit_executor_) async_commit_executor_->WakeAll();
  ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                  "Async commit stopped; reopen required: %s",
                  status.ToString().c_str());
}

Status DBImpl::PublishAsyncCommitSequence(SequenceNumber sequence) {
  SequenceNumber previous;
  {
    std::lock_guard<std::mutex> lock(async_commit_mutex_);
    if (!async_commit_error_.ok()) return async_commit_error_;
    if (async_commit_closing_) return Status::ShutdownInProgress();
    assert(sequence >= versions_->LastSequence());
    previous = versions_->LastSequence();
    versions_->SetLastSequence(sequence);
  }
  async_commit_cv_.notify_all();
  SignalAsyncCommitPublication(previous, sequence);
  return Status::OK();
}

Status DBImpl::PrepareAsyncCommitRead(const ReadOptions& options) {
  if (!immutable_db_options_.enable_async_commit ||
      immutable_db_options_.async_commit_strong_visibility)
    return Status::OK();
  // Capture once, before selecting a read view or doing any lookup. Never
  // chase writes allocated after this boundary. The max includes recovery and
  // sequence-consuming management operations preceding the first async write.
  SequenceNumber target =
      std::max(async_commit_allocated_sequence_.load(std::memory_order_acquire),
               versions_->LastSequence());
  if (versions_->LastSequence() >= target) return Status::OK();
  TEST_SYNC_POINT_CALLBACK("DBImpl::PrepareAsyncCommitRead:Wait", &target);
  return WaitForAsyncCommitSequence(target, options);
}

Status DBImpl::WaitForAsyncCommitSequence(SequenceNumber target,
                                          const ReadOptions& options) {
  if (versions_->LastSequence() >= target) return Status::OK();
  const uint32_t spin_rounds =
      immutable_db_options_.async_commit_wait_spin_rounds;
  const bool has_deadline = options.deadline.count() != 0;
  auto check_deadline = [&]() {
    return has_deadline &&
           (options.deadline.count() < 0 ||
            immutable_db_options_.clock->NowMicros() >=
                static_cast<uint64_t>(options.deadline.count()));
  };
  if (async_commit_wait_stopped_.load(std::memory_order_acquire))
    return AsyncCommitStatus();
  if (check_deadline())
    return Status::TimedOut("waiting for async commit publication");
  const uint32_t spin_delay = immutable_db_options_.async_commit_wait_spin_delay;
  const uint32_t pause_multiplier =
      immutable_db_options_.async_commit_wait_spin_pause_multiplier;
  Random* random = spin_rounds != 0 && spin_delay != 0 && pause_multiplier != 0
                       ? Random::GetTLSInstance()
                       : nullptr;
  // InnoDB-style outer predicate polls with randomized, grouped CPU delay.
  // The acquire predicate supplies publication synchronization; an additional
  // os_rmb is unnecessary. The normal inner loop touches only local state.
  for (uint32_t round = 0; round < spin_rounds; ++round) {
    if (versions_->LastSequence() >= target) return Status::OK();
    if (async_commit_wait_stopped_.load(std::memory_order_acquire))
      return AsyncCommitStatus();
    if (has_deadline && round != 0 && (round & 63U) == 0 && check_deadline())
      return Status::TimedOut("waiting for async commit publication");
    // Validation bounds the product to 999 * 100, including at UINT32_MAX
    // outer rounds. Uniform's upper bound is exclusive, like ut_delay's caller.
    uint32_t pauses = random ? random->Uniform(spin_delay) * pause_multiplier : 0;
    TEST_SYNC_POINT_CALLBACK("DBImpl::WaitForAsyncCommitSequence:Spin", &pauses);
    if (!has_deadline) {
      for (uint32_t pause = 0; pause < pauses; ++pause) {
        TEST_SYNC_POINT("DBImpl::WaitForAsyncCommitSequence:Pause");
        port::AsmVolatilePause();
      }
    } else {
      // Explicit deadlines may interrupt a long delay. Keep clock/predicate
      // checks outside the normal no-deadline inner loop.
      while (pauses != 0) {
        const uint32_t chunk = std::min(pauses, 64U);
        for (uint32_t pause = 0; pause < chunk; ++pause) {
          TEST_SYNC_POINT("DBImpl::WaitForAsyncCommitSequence:Pause");
          port::AsmVolatilePause();
        }
        pauses -= chunk;
        if (versions_->LastSequence() >= target) return Status::OK();
        if (async_commit_wait_stopped_.load(std::memory_order_acquire))
          return AsyncCommitStatus();
        if (check_deadline())
          return Status::TimedOut("waiting for async commit publication");
      }
    }
  }
  if (versions_->LastSequence() >= target) return Status::OK();
  if (async_commit_wait_stopped_.load(std::memory_order_acquire))
    return AsyncCommitStatus();
  if (check_deadline())
    return Status::TimedOut("waiting for async commit publication");
  uint32_t index = target % immutable_db_options_.async_commit_wait_slots;
  TEST_SYNC_POINT_CALLBACK("DBImpl::WaitForAsyncCommitSequence:BeforeSlotLock",
                           &index);
  auto& slot = async_commit_wait_slots_[index];
  std::unique_lock<std::mutex> lock(slot.mutex);
  // The notifier locks every slot crossed by P, even an empty one. Together
  // with this locked predicate recheck this closes the check-to-sleep race.
  if (versions_->LastSequence() >= target) return Status::OK();
  ++slot.waiters;
  async_commit_sequence_notifier_->RegisterWaiter();
  struct Registration {
    size_t& waiters;
    AsyncCommitSequenceNotifier* notifier;
    ~Registration() {
      --waiters;
      notifier->UnregisterWaiter();
    }
  } registration{slot.waiters, async_commit_sequence_notifier_.get()};
  while (versions_->LastSequence() < target) {
    if (async_commit_wait_stopped_.load(std::memory_order_acquire))
      return AsyncCommitStatus();
    TEST_SYNC_POINT_CALLBACK("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                             &index);
    if (options.deadline.count() != 0) {
      uint64_t now = immutable_db_options_.clock->NowMicros();
      if (options.deadline.count() <= 0 ||
          now >= static_cast<uint64_t>(options.deadline.count())) {
        return Status::TimedOut("waiting for async commit publication");
      }
      // Use the system_clock overload, which maps to pthread_cond_timedwait
      // even on libstdc++ versions whose steady-clock wait is not intercepted
      // by older thread sanitizers. Keep the configured clock's remaining
      // duration, and recheck its absolute deadline on every wakeup.
      slot.cv.wait_until(
          lock, std::chrono::system_clock::now() +
                    std::chrono::microseconds(
                        static_cast<uint64_t>(options.deadline.count()) - now));
    } else {
      slot.cv.wait(lock);
    }
  }
  return Status::OK();
}

void DBImpl::SignalAsyncCommitPublication(SequenceNumber previous,
                                          SequenceNumber published) {
  assert(published >= previous);
  if (published > previous) {
    async_commit_sequence_notifier_->PublicationAdvanced();
    TEST_SYNC_POINT_CALLBACK("DBImpl::SignalAsyncCommitPublication:Published",
                             &published);
  }
}

void DBImpl::NotifyAsyncCommitPublication(SequenceNumber previous,
                                          SequenceNumber published) {
  assert(published >= previous);
  const uint32_t slots = immutable_db_options_.async_commit_wait_slots;
  const uint32_t count = static_cast<uint32_t>(
      std::min<SequenceNumber>(published - previous, slots));
  uint32_t index = (previous + 1) % slots;
  for (uint32_t i = 0; i < count; ++i, index = (index + 1) % slots) {
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::NotifyAsyncCommitPublication:BeforeSlotLock", &index);
    auto& slot = async_commit_wait_slots_[index];
    std::lock_guard<std::mutex> lock(slot.mutex);
    if (slot.waiters != 0) slot.cv.notify_all();
  }
}

void DBImpl::NotifyAllAsyncCommitWaiters() {
  for (uint32_t i = 0; i < immutable_db_options_.async_commit_wait_slots; ++i) {
    auto& slot = async_commit_wait_slots_[i];
    std::lock_guard<std::mutex> lock(slot.mutex);
    if (slot.waiters != 0) slot.cv.notify_all();
  }
}

Status DBImpl::ReserveAsyncCommitBytes(size_t bytes, size_t already_reserved,
                                       bool no_slowdown) {
  std::unique_lock<std::mutex> lock(async_commit_mutex_);
  assert(async_commit_pending_bytes_ >= already_reserved);
  const size_t limit = immutable_db_options_.max_async_commit_pending_bytes;
  for (;;) {
    if (!async_commit_error_.ok()) return async_commit_error_;
    if (async_commit_closing_) return Status::ShutdownInProgress();
    const size_t other = async_commit_pending_bytes_ - already_reserved;
    // An oversized group may exclusively hold the budget, preserving large
    // writes without allowing an unbounded queue of owned payloads.
    const bool pool = immutable_db_options_.async_commit_memtable_threads > 0;
    const size_t other_groups =
        async_commit_pending_groups_ - (already_reserved != 0);
    const bool group_available =
        !pool ||
        other_groups < immutable_db_options_.max_async_commit_pending_groups;
    const bool alone = pool ? other_groups == 0 : other == 0;
    if (group_available &&
        (alone || (bytes <= limit && other <= limit - bytes))) {
      if (bytes > std::numeric_limits<size_t>::max() - other) {
        return Status::InvalidArgument(
            "async commit memory accounting overflow");
      }
      async_commit_pending_bytes_ = other + bytes;
      if (already_reserved == 0) ++async_commit_pending_groups_;
      return Status::OK();
    }
    if (no_slowdown)
      return Status::Incomplete("async commit pending byte limit");
    TEST_SYNC_POINT("DBImpl::AsyncWriteImpl:CapacityWait");
    async_commit_cv_.wait(lock);
  }
}

void DBImpl::ReleaseAsyncCommitResources(AsyncCommitGroup* group) {
  ReleaseAsyncCommitResources(&group, 1);
}

void DBImpl::ReleaseAsyncCommitResources(AsyncCommitGroup* const* groups,
                                         size_t count) {
  for (size_t i = 0; i < count; ++i) {
    auto* group = groups[i];
    group->merged_batch.reset();
    if (group->inline_batch) *group->inline_batch = WriteBatch();
    group->apply_batch = nullptr;
    std::vector<AsyncCommitGroup::Range>().swap(group->ranges);
  }
  {
    InstrumentedMutexLock lock(&mutex_);
    for (size_t i = 0; i < count; ++i) {
      auto* group = groups[i];
      // A serial queue task covers the entire group. Its final scheduling
      // check shares this lock with reference retirement, before P/drain can
      // advance. Rejected, never-queued groups have no insertion to schedule.
      if (group->serial && group->queued) ScheduleAsyncCommitTargets(*group);
      for (const auto& target : group->targets) {
        // The current memtable still owns a reference: all switches drain
        // first.
        MemTable* obsolete = target.mem->Unref();
        assert(obsolete == nullptr);
        delete obsolete;
      }
      for (auto* cfd : group->cf_refs) cfd->UnrefAndTryDelete();
    }
  }
  for (size_t i = 0; i < count; ++i) {
    std::vector<AsyncCommitGroup::Target>().swap(groups[i]->targets);
    std::vector<ColumnFamilyData*>().swap(groups[i]->cf_refs);
  }
}

void DBImpl::ReleaseAsyncCommitGroup(AsyncCommitGroup* group) {
  ReleaseAsyncCommitResources(group);
  if (group->reserved_bytes != 0) {
    {
      std::lock_guard<std::mutex> lock(async_commit_mutex_);
      assert(async_commit_pending_bytes_ >= group->reserved_bytes);
      async_commit_pending_bytes_ -= group->reserved_bytes;
      --async_commit_pending_groups_;
    }
    group->reserved_bytes = 0;
    async_commit_cv_.notify_all();
  }
}

Status DBImpl::BuildAsyncCommitGroup(WriteThread::WriteGroup& writers,
                                     AsyncCommitGroup* group) {
  size_t merged_bytes = WriteBatchInternal::kHeader;
  group->ranges.reserve(writers.size);
  {
    // CF option mutation uses this mutex. Keep a reference independent of all
    // caller handles; Drop/switch additionally drain the representative queue.
    InstrumentedMutexLock lock(&mutex_);
    for (auto* writer : writers) {
      WriteBatch* batch = writer->batch;
      const size_t size = WriteBatchInternal::ByteSize(batch);
      if (size < WriteBatchInternal::kHeader) {
        return Status::Corruption("short async commit batch");
      }
      const uint32_t count = WriteBatchInternal::Count(batch);
      Status status = WriteBatchInternal::ValidateAsyncCommitRange(
          batch, WriteBatchInternal::kHeader, size, 0, count);
      if (!status.ok()) return status;
      AsyncBatchValidator validator(versions_->GetColumnFamilySet(),
                                    &group->cf_refs,
                                    writer->ignore_missing_column_families);
      status = batch->Iterate(&validator);
      if (!status.ok()) return status;
      if (count > std::numeric_limits<uint32_t>::max() - group->record_count ||
          size - WriteBatchInternal::kHeader >
              std::numeric_limits<size_t>::max() - merged_bytes) {
        return Status::InvalidArgument(
            "async commit group size/count overflow");
      }
      AsyncCommitGroup::Range range;
      range.begin_offset = merged_bytes;
      merged_bytes += size - WriteBatchInternal::kHeader;
      range.end_offset = merged_bytes;
      range.first_record_index = group->record_count;
      range.record_count = count;
      range.ignore_missing_column_families =
          writer->ignore_missing_column_families;
      range.hint_per_batch = writer->memtable_insert_hint_per_batch;
      group->ranges.push_back(range);
      group->record_count += count;
      group->exclusive = group->exclusive || batch->HasMerge();
    }
    if (immutable_db_options_.async_commit_memtable_threads > 0) {
      group->targets.reserve(group->cf_refs.size());
      for (auto* cfd : group->cf_refs) {
        cfd->mem()->Ref();
        group->targets.push_back({cfd, cfd->mem(), cfd->GetLogNumber()});
      }
    }
  }
  // Charge conservative allocation capacities, including checksums and the
  // descriptor/reference arrays. Only the no-pool single-request path borrows
  // caller storage.
  const size_t task_storage =
      immutable_db_options_.async_commit_memtable_threads > 0
          ? AsyncCommitExecutor::TaskStorageBytes(
                group->ranges.size(),
                group->exclusive ||
                    immutable_db_options_.async_commit_memtable_threads == 1)
          : 0;
  const size_t metadata =
      group->MetadataBytes() +
      group->ranges.capacity() * sizeof(AsyncCommitGroup::Range) +
      group->cf_refs.capacity() * sizeof(ColumnFamilyData*) +
      group->targets.capacity() * sizeof(AsyncCommitGroup::Target) +
      task_storage;
  const size_t protection =
      writers.leader->protection_bytes_per_key
          ? sizeof(WriteBatch::ProtectionInfo) +
                (group->record_count + 8) * sizeof(ProtectionInfoKVOC64)
          : 0;
  if (merged_bytes >
      (std::numeric_limits<size_t>::max() - metadata - protection) / 2) {
    return Status::InvalidArgument("async commit group capacity overflow");
  }
  size_t bytes = metadata;
  const bool owned = immutable_db_options_.async_commit_memtable_threads > 0;
  if (writers.size > 1 || owned) {
    bytes += (group->inline_batch ? 0 : sizeof(WriteBatch)) +
             2 * (merged_bytes + protection);
  }
  Status status =
      ReserveAsyncCommitBytes(bytes, 0, writers.leader->no_slowdown);
  if (!status.ok()) return status;
  group->reserved_bytes = bytes;
  if (writers.size == 1 && !owned) {
    group->apply_batch = writers.leader->batch;
  } else {
    if (group->inline_batch) {
      *group->inline_batch =
          WriteBatch(merged_bytes, 0, writers.leader->protection_bytes_per_key);
      group->apply_batch = group->inline_batch;
    } else {
      group->merged_batch.reset(new WriteBatch(
          merged_bytes, 0, writers.leader->protection_bytes_per_key));
      group->apply_batch = group->merged_batch.get();
    }
    WriteBatchInternal::ReserveForAppend(group->apply_batch,
                                         group->record_count);
    for (auto* writer : writers) {
      status = WriteBatchInternal::Append(group->apply_batch, writer->batch);
      if (!status.ok()) return status;
    }
    size_t actual = metadata +
                    WriteBatchInternal::AllocatedBytes(group->apply_batch) -
                    (group->inline_batch ? sizeof(WriteBatch) : 0);
    if (actual > bytes) {
      status =
          ReserveAsyncCommitBytes(actual, bytes, writers.leader->no_slowdown);
      if (!status.ok()) return status;
      group->reserved_bytes = actual;
    }
  }
  return Status::OK();
}

Status DBImpl::ApplyAsyncCommitGroup(const AsyncCommitGroup& group) {
  size_t expected_offset = WriteBatchInternal::kHeader;
  size_t expected_index = 0;
  for (const auto& range : group.ranges) {
    if (range.begin_offset != expected_offset ||
        range.first_record_index != expected_index) {
      return Status::Corruption("noncontiguous async commit ranges");
    }
    Status status = WriteBatchInternal::InsertIntoRange(
        group.apply_batch, range.begin_offset, range.end_offset,
        range.first_record_index, range.record_count, range.first_sequence,
        column_family_memtables_.get(), &flush_scheduler_,
        &trim_history_scheduler_, range.ignore_missing_column_families, this,
        range.hint_per_batch);
    if (!status.ok()) return status;
    expected_offset = range.end_offset;
    expected_index += range.record_count;
    TEST_SYNC_POINT("DBImpl::AsyncWriteImpl:RangeApplied");
  }
  if (expected_offset != WriteBatchInternal::ByteSize(group.apply_batch) ||
      expected_index != WriteBatchInternal::Count(group.apply_batch)) {
    return Status::Corruption("incomplete async commit range coverage");
  }
  return Status::OK();
}

Status DBImpl::AsyncWriteImpl(const WriteOptions& options, WriteBatch* batch,
                              uint64_t* log_used, uint64_t* seq_used) {
  const bool strong_visibility =
      immutable_db_options_.async_commit_strong_visibility;
  Status status = AsyncCommitStatus();
  if (!status.ok()) return status;
  PERF_TIMER_GUARD(write_pre_and_post_process_time);
  StopWatch write_sw(immutable_db_options_.clock, stats_, DB_WRITE);
  WriteThread::Writer writer(options, batch, nullptr, 0, false);
  write_thread_.JoinBatchGroup(&writer);
  auto finish = [&]() {
    Status result = writer.FinalStatus();
    if (result.ok()) {
      if (strong_visibility) {
        // Every caller has left the WAL queue before entering this wait. The
        // worker pool publishes the ordered prefix independently of callers.
        result = WaitForAsyncCommitSequence(writer.async_end_sequence,
                                            ReadOptions());
      }
    }
    if (log_used) *log_used = writer.log_used;
    if (seq_used) *seq_used = writer.sequence;
    return result;
  };
  if (writer.state == WriteThread::STATE_WAL_COMPLETED ||
      writer.state == WriteThread::STATE_COMPLETED)
    return finish();
  assert(writer.state == WriteThread::STATE_GROUP_LEADER);

  WriteContext write_context;
  WriteThread::WriteGroup wal_group;
  AsyncCommitGroup local_group;  // Preserve V1's stack-owned fast path.
  std::shared_ptr<AsyncCommitGroup> owned_group;
  if (async_commit_executor_) {
    if (immutable_db_options_.async_commit_memtable_threads == 1)
      owned_group = std::make_shared<AsyncCommitSerialGroup>();
    else
      owned_group = std::make_shared<AsyncCommitGroup>();
  }
  AsyncCommitGroup& group = owned_group ? *owned_group : local_group;
  bool need_log_sync = options.sync;
  bool need_log_dir_sync = false;
  log::Writer* log_writer = nullptr;
  uint64_t log_number = 0;
  {
    InstrumentedMutexLock lock(&mutex_);
    need_log_dir_sync = need_log_sync && !log_dir_synced_;
    status = PreprocessWrite(options, &need_log_sync, &write_context);
    log_writer = logs_.back().writer;
    log_number = logfile_number_;
  }
  TEST_SYNC_POINT("DBImpl::AsyncWriteImpl:BeforeGroup");
  last_batch_group_size_ =
      write_thread_.EnterAsBatchGroupLeader(&writer, &wal_group);
  if (status.ok()) status = AsyncCommitStatus();
  if (status.ok()) status = BuildAsyncCommitGroup(wal_group, &group);
  if (status.ok() && owned_group)
    status = async_commit_executor_->PrepareGroup(owned_group);
  if (status.ok()) {
    const SequenceNumber allocated =
        write_thread_.UpdateLastSequence(versions_->LastSequence());
    if (group.record_count > kMaxSequenceNumber - allocated) {
      status = Status::InvalidArgument("async commit sequence exhausted");
    } else {
      group.last_sequence = allocated + group.record_count;
      SequenceNumber next = allocated + (group.record_count != 0);
      size_t i = 0;
      for (auto* w : wal_group) {
        auto& range = group.ranges[i++];
        range.first_sequence = std::min(next, kMaxSequenceNumber);
        w->sequence = range.first_sequence;
        w->async_has_sequence = range.record_count != 0;
        // Metadata-only callers consume no sequence, but strong visibility
        // still waits for preceding writes. Group publication is atomic, so
        // using the group's end for those callers is conservative and safe.
        w->async_end_sequence = w->async_has_sequence
                                    ? next + range.record_count - 1
                                    : group.last_sequence;
        w->log_used = log_number;
        next += range.record_count;
      }
      // Recovery computes the last sequence as the inserter's next sequence
      // minus one. Empty/LogData-only batches must therefore start at A+1 too,
      // or trailing metadata would hide the last acknowledged update on reopen.
      // A=kMaxSequenceNumber is safe: no records consume A+1 in this batch.
      WriteBatchInternal::SetSequence(group.apply_batch, allocated + 1);
      write_thread_.UpdateLastSequence(group.last_sequence);
      async_commit_allocated_sequence_.store(group.last_sequence,
                                             std::memory_order_release);
      TEST_SYNC_POINT("DBImpl::AsyncWriteImpl:SequenceAllocated");
      {
        PERF_TIMER_GUARD(write_wal_time);
        IOStatus io_status = WritePreparedBatchToWAL(
            *group.apply_batch, log_writer, log_used, need_log_sync,
            need_log_dir_sync, wal_group.size);
        TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:WALStatus",
                                 &io_status);
        status = io_status;
        if (!io_status.ok()) {
          FailAsyncCommit(status);
          IOStatusCheck(io_status);
        }
      }
      if (!status.ok()) FailAsyncCommit(status);
    }
  }
  if (need_log_sync) {
    InstrumentedMutexLock lock(&mutex_);
    if (status.ok()) {
      status = MarkLogsSynced(log_number, need_log_dir_sync);
      if (!status.ok()) FailAsyncCommit(status);
    } else {
      MarkLogsNotSynced(log_number);
    }
  }
  writer.status = status;
  if (!status.ok()) {
    write_thread_.DetachAsyncFollowersKeepLeader(wal_group, status);
    TEST_SYNC_POINT("DBImpl::AsyncWriteImpl:RejectedGroupCleanup");
    // Keep the WAL gate until rejected targets are released. A prepared V3
    // record must be cancelled as well, so no incomplete frontier entry leaks.
    if (owned_group && group.group_id != 0)
      async_commit_executor_->CancelPreparedGroup(owned_group);
    else
      ReleaseAsyncCommitGroup(&group);
    write_thread_.ExitUnbatched(&writer);
    return finish();
  }

  const size_t original_count = wal_group.size;
  auto* stats = default_cf_internal_stats_;
  stats->AddDBStats(InternalStats::kIntStatsNumKeysWritten, group.record_count);
  stats->AddDBStats(InternalStats::kIntStatsBytesWritten,
                    WriteBatchInternal::ByteSize(group.apply_batch));
  stats->AddDBStats(InternalStats::kIntStatsWriteDoneBySelf, 1);
  stats->AddDBStats(InternalStats::kIntStatsWriteDoneByOther,
                    original_count - 1);
  RecordTick(stats_, NUMBER_KEYS_WRITTEN, group.record_count);
  RecordTick(stats_, BYTES_WRITTEN,
             WriteBatchInternal::ByteSize(group.apply_batch));
  RecordTick(stats_, WRITE_DONE_BY_SELF, 1);
  RecordTick(stats_, WRITE_DONE_BY_OTHER, original_count - 1);
  TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:WALComplete",
                           group.apply_batch);
  if (async_commit_executor_) {
    async_commit_executor_->SubmitPreparedGroup(owned_group);
    // Sync writes benefit from notifying this group's callers before the next
    // WAL snapshot. Without sync, that serialization makes the short WAL path
    // pay for every follower notification before progressing to the next group.
    // Both policies enqueue first and release the WAL gate before finish()
    // optionally waits for memtable sequence publication.
    if (options.sync)
      write_thread_.DetachAsyncFollowersKeepLeader(wal_group, Status::OK());
    TEST_SYNC_POINT("AsyncCommitExecutor::Submit:BeforeWake");
    async_commit_executor_->WakeAll();
    if (options.sync)
      write_thread_.ExitUnbatched(&writer);
    else
      write_thread_.ExitAsyncWalGroup(wal_group, Status::OK());
    return finish();
  }
  if (immutable_db_options_.enable_pipelined_write) {
    group.memtable_writer.batch = group.apply_batch;
    write_thread_.TransferAsyncGroupToMemtable(wal_group,
                                               &group.memtable_writer);
    write_thread_.WaitForAsyncMemtableWriter(&group.memtable_writer);
  } else {
    write_thread_.DetachAsyncFollowersKeepLeader(wal_group, Status::OK());
  }
  status = AsyncCommitStatus();
  if (status.ok()) {
    PERF_TIMER_GUARD(write_memtable_time);
    TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:BeforeApply",
                             group.apply_batch);
    status = AsyncCommitStatus();
    if (status.ok()) status = ApplyAsyncCommitGroup(group);
    TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:ApplyStatus", &status);
    if (status.ok()) status = PublishAsyncCommitSequence(group.last_sequence);
    if (!status.ok()) {
      FailAsyncCommit(status);
      InstrumentedMutexLock lock(&mutex_);
      error_handler_.SetBGError(status, BackgroundErrorReason::kMemTable)
          .PermitUncheckedError();
    }
  }
  writer.status = status;
  if (immutable_db_options_.enable_pipelined_write) {
    WriteThread::WriteGroup mem_group;
    mem_group.leader = mem_group.last_writer = &group.memtable_writer;
    mem_group.size = 1;
    mem_group.status = status;
    write_thread_.ExitAsMemTableWriter(&group.memtable_writer, mem_group);
  } else {
    write_thread_.ExitUnbatched(&writer);
  }
  ReleaseAsyncCommitGroup(&group);
  return finish();
}

namespace {
class AsyncCommitMemTables : public ColumnFamilyMemTables {
 public:
  explicit AsyncCommitMemTables(const AsyncCommitGroup& group)
      : group_(group) {}
  bool Seek(uint32_t id) override {
    current_ = nullptr;
    for (const auto& target : group_.targets) {
      if (target.cfd->GetID() == id) {
        current_ = &target;
        break;
      }
    }
    handle_.SetCFD(current_ ? current_->cfd : nullptr);
    return current_ != nullptr;
  }
  uint64_t GetLogNumber() const override { return current_->log_number; }
  MemTable* GetMemTable() const override { return current_->mem; }
  ColumnFamilyHandle* GetColumnFamilyHandle() override { return &handle_; }
  ColumnFamilyData* current() override {
    return current_ ? current_->cfd : nullptr;
  }

 private:
  const AsyncCommitGroup& group_;
  const AsyncCommitGroup::Target* current_ = nullptr;
  ColumnFamilyHandleInternal handle_;
};
}  // namespace

Status DBImpl::ApplyAsyncCommitRange(const AsyncCommitGroup& group,
                                     size_t index, bool concurrent) {
  const auto& range = group.ranges[index];
  SequenceNumber sequence = range.first_sequence;
  TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:BeforeRange", &sequence);
  AsyncCommitMemTables memtables(group);
  Status status = WriteBatchInternal::InsertIntoRange(
      group.apply_batch, range.begin_offset, range.end_offset,
      range.first_record_index, range.record_count, range.first_sequence,
      &memtables, nullptr, nullptr, range.ignore_missing_column_families, this,
      range.hint_per_batch, concurrent);
  TEST_SYNC_POINT_CALLBACK("DBImpl::AsyncWriteImpl:RangeComplete", &sequence);
  TEST_SYNC_POINT("DBImpl::AsyncWriteImpl:RangeApplied");
  return status;
}

void DBImpl::ScheduleAsyncCommitTargets(const AsyncCommitGroup& group) {
  mutex_.AssertHeld();
  for (const auto& target : group.targets) {
    auto* cfd = target.cfd;
    assert(cfd->mem() == target.mem);
    if (target.mem->ShouldScheduleFlush() && target.mem->MarkFlushScheduled())
      flush_scheduler_.ScheduleWork(cfd);
    const size_t maintain = cfd->ioptions()->max_write_buffer_size_to_maintain;
    auto* imm = cfd->imm();
    if (maintain > 0 && imm->HasHistory() &&
        target.mem->ApproximateMemoryUsageFast() +
                imm->ApproximateMemoryUsageExcludingLast() >=
            maintain &&
        imm->MarkTrimHistoryNeeded()) {
      trim_history_scheduler_.ScheduleWork(cfd);
    }
  }
}

void DBImpl::StartAsyncCommitExecutor() {
  if (!immutable_db_options_.enable_async_commit) return;
  async_commit_sequence_notifier_.reset(new AsyncCommitSequenceNotifier(this));
  async_commit_sequence_notifier_->Start(versions_->LastSequence());
  if (immutable_db_options_.async_commit_memtable_threads == 0) return;
  async_commit_executor_.reset(new AsyncCommitExecutor(this));
  async_commit_executor_->Start();
  write_thread_.SetAsyncCommitDrain([this] {
    DrainAsyncCommits(async_commit_executor_->LastSubmitted())
        .PermitUncheckedError();
  });
}

Status DBImpl::StopAsyncCommitExecutor() {
  return async_commit_executor_
             ? async_commit_executor_->BeginShutdownAndDrain()
             : Status::OK();
}

void DBImpl::StopAsyncCommitSequenceNotifier() {
  if (async_commit_sequence_notifier_) async_commit_sequence_notifier_->Stop();
}

Status DBImpl::DrainAsyncCommits(uint64_t target_group_id) {
  return async_commit_executor_
             ? async_commit_executor_->DrainThrough(target_group_id)
             : Status::OK();
}

void DBImpl::DrainAsyncCommitsWithDBMutex() {
  mutex_.AssertHeld();
  uint64_t target = async_commit_executor_->LastSubmitted();
  mutex_.Unlock();
  DrainAsyncCommits(target).PermitUncheckedError();
  mutex_.Lock();
}

Status DBImpl::WaitForAsyncCommit() {
  if (!immutable_db_options_.enable_async_commit) return Status::OK();
  InstrumentedMutexLock lock(&mutex_);
  WriteThread::Writer barrier;
  write_thread_.EnterUnbatched(&barrier, &mutex_);
  Status status = AsyncCommitStatus(false);
  write_thread_.ExitUnbatched(&barrier);
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
