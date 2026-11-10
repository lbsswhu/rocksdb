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
  size_t index = 0;
  AsyncCommitTask() = default;
  AsyncCommitTask(const std::shared_ptr<AsyncCommitGroup>& owner, size_t range)
      : group(owner), index(range) {}
};
using AsyncCommitTaskQueue = AsyncCommitQueue<AsyncCommitTask>;

// Stable storage, independent of every caller's Writer/batch/CF handle. Fields
// complete/next are protected by DBImpl::async_commit_mutex_. Targets are
// captured and released under the DB mutex, and never changed during insertion.
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
  bool serial = false;
  uint64_t group_id = 0;
  uint64_t queued_micros = 0;
  std::shared_ptr<AsyncCommitGroup> next;
  uint64_t previous_barrier = 0;
  AsyncCommitTaskQueue::Node* first_task = nullptr;
  AsyncCommitTaskQueue::Node* last_task = nullptr;
  std::atomic<size_t> remaining{0};
  bool queued = false;
  bool complete = false;
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

// V3: pre-WAL preparation registers completion records under the async mutex.
// Post-WAL publication and task dispatch use an independent lock-free FIFO.
// Long-lived workers poll briefly before sleeping on an epoch-counted event.
class AsyncCommitExecutor {
 public:
  explicit AsyncCommitExecutor(DBImpl* db);
  ~AsyncCommitExecutor();
  void Start();
  Status PrepareGroup(const std::shared_ptr<AsyncCommitGroup>& group);
  void CancelPreparedGroup(const std::shared_ptr<AsyncCommitGroup>& group);
  void SubmitPreparedGroup(const std::shared_ptr<AsyncCommitGroup>& group);
  static size_t TaskStorageBytes(size_t ranges, bool whole_group);
  uint64_t LastSubmitted();
  Status DrainThrough(uint64_t group_id);
  Status BeginShutdownAndDrain();
  void WakeAll();
  bool GetIntProperty(const Slice& name, uint64_t* value);

 private:
  void Run(size_t worker);
  bool TakeOrWait(size_t worker, AsyncCommitTask* task);
  void WaitForFence(const AsyncCommitTask& task);
  void Complete(const std::shared_ptr<AsyncCommitGroup>& group);
  void Complete(const AsyncCommitTask* tasks, size_t count);
  DBImpl* const db_;
  const size_t workers_;
  ThreadPoolImpl pool_;
  AsyncCommitTaskQueue queue_;
  AsyncCommitEvent event_;
  std::shared_ptr<AsyncCommitGroup> head_, tail_;
  uint64_t prepared_ = 0;  // producer only; completion links use async mutex
  uint64_t last_exclusive_ = 0;  // producer only
  uint64_t drained_ticket_ = 0;  // async mutex
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
