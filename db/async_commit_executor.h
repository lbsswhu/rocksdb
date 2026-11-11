// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "db/column_family.h"
#include "db/write_thread.h"
#include "rocksdb/write_batch.h"
#include "util/threadpool_imp.h"
#include "util/async_commit_queue.h"
#include "util/async_commit_event.h"

namespace ROCKSDB_NAMESPACE {
class DBImpl;
struct AsyncCommitGroup;
struct AsyncCommitTask {
  std::shared_ptr<AsyncCommitGroup> group;
  AsyncCommitTask() = default;
  explicit AsyncCommitTask(const std::shared_ptr<AsyncCommitGroup>& owner)
      : group(owner) {}
};
using AsyncCommitTaskQueue = AsyncCommitQueue<AsyncCommitTask>;

// Stable storage, independent of every caller's Writer/batch/CF handle. Fields
// next is owned by the multi-worker notifier. completion_owner keeps a report
// alive from preparation until the notifier takes ownership. Targets are
// captured/released under the DB mutex and stay stable during insertion.
struct AsyncCommitGroup {
  struct Range {
    size_t begin_offset;
    size_t end_offset;
    size_t first_record_index;
    uint32_t record_count;
    SequenceNumber first_sequence = 0;
    bool ignore_missing_column_families;
    bool hint_per_batch;
  };
  struct Target {
    ColumnFamilyData* cfd;
    MemTable* mem;
    uint64_t log_number;
  };
  std::unique_ptr<WriteBatch> merged_batch;
  WriteBatch* inline_batch = nullptr;  // storage supplied by the serial subtype
  WriteBatch* apply_batch = nullptr;
  std::vector<Range> ranges;
  std::vector<ColumnFamilyData*> cf_refs;
  std::vector<Target> targets;
  WriteThread::Writer memtable_writer;  // V1 only; never queued in pool mode.
  SequenceNumber last_sequence = 0;
  size_t record_count = 0;
  size_t reserved_bytes = 0;
  bool exclusive = false;
  uint64_t group_id = 0;
  uint64_t queued_micros = 0;
  std::shared_ptr<AsyncCommitGroup> next;
  std::shared_ptr<AsyncCommitGroup> completion_owner;
  AsyncCommitGroup* completion_next = nullptr;
  uint64_t previous_barrier = 0;
  AsyncCommitTaskQueue::Node* first_task = nullptr;
  bool queued = false;
  size_t MetadataBytes() const;
};

// make_shared keeps the owned batch object in the same allocation as the serial
// group. The shared_ptr control block retains this subtype's destructor.
struct AsyncCommitSerialGroup : AsyncCommitGroup {
  AsyncCommitSerialGroup() { inline_batch = &batch; }
  WriteBatch batch;
};

inline size_t AsyncCommitGroup::MetadataBytes() const {
  return (inline_batch ? sizeof(AsyncCommitSerialGroup) : sizeof(AsyncCommitGroup)) +
         4 * sizeof(std::shared_ptr<AsyncCommitGroup>);
}

// One whole WAL group per work item. Single-worker completion publishes P
// directly; multi-worker reports are ordered and published by the notifier.
class AsyncCommitExecutor {
 public:
  explicit AsyncCommitExecutor(DBImpl* db);
  ~AsyncCommitExecutor();
  void Start();
  Status PrepareGroup(const std::shared_ptr<AsyncCommitGroup>& group);
  void CancelPreparedGroup(const std::shared_ptr<AsyncCommitGroup>& group);
  void SubmitPreparedGroup(const std::shared_ptr<AsyncCommitGroup>& group);
  static size_t TaskStorageBytes();
  uint64_t LastSubmitted();
  Status DrainThrough(uint64_t group_id);
  Status BeginShutdownAndDrain();
  void WakeAll();
  void WakeOne();
  bool GetIntProperty(const Slice& name, uint64_t* value);

 private:
  friend class AsyncCommitSequenceNotifier;
  void Run(size_t worker);
  void RunSerial(size_t worker);
  bool TakeOrWait(size_t worker, AsyncCommitTask* task);
  void WaitForFence(const AsyncCommitTask& task);
  void ApplyGroup(AsyncCommitGroup& group);
  void Complete(const std::shared_ptr<AsyncCommitGroup>& group);
  // Caller holds async_commit_mutex_. Apply and visibility can precede cleanup.
  void PublishCompletedGroup(const AsyncCommitGroup& group);
  // Caller holds async_commit_mutex_ and has released all group resources.
  void RetireCleanedGroups(AsyncCommitGroup* const* groups, size_t count);
  void RetireCompletedGroup(AsyncCommitGroup* group);
  DBImpl* const db_;
  const size_t workers_;
  ThreadPoolImpl pool_;
  AsyncCommitTaskQueue queue_;
  AsyncCommitEvent event_;
  uint64_t prepared_ = 0;        // successful submissions, WAL producer only
  uint64_t last_exclusive_ = 0;  // producer only
  uint64_t drained_ticket_ = 0;  // async mutex
  uint64_t published_ticket_ = 0;  // async mutex; includes zero-record groups
  size_t started_workers_ = 0;  // async mutex
  std::atomic<uint64_t> submitted_{0}, last_submitted_{0}, drained_{0};
  std::atomic<bool> accepting_{true}, stop_{false};
  bool joined_ = false;
  std::atomic<uint64_t> lease_batches_{0}, leased_ranges_{0}, leased_bytes_{0};
  std::atomic<uint64_t> single_copy_bytes_{0}, queue_micros_{0}, apply_micros_{0};
  std::atomic<uint64_t> idle_polls_{0}, sleep_calls_{0}, wake_calls_{0};
  std::atomic<uint64_t> submit_micros_{0};
  uint64_t idle_micros_ = 50;
};
}  // namespace ROCKSDB_NAMESPACE
