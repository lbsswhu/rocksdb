// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <map>
#if defined(__linux__)
#include <sys/wait.h>
#include <unistd.h>
#endif
#include <mutex>
#include <thread>

#include "db/async_commit_sequence_notifier.h"
#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "db/write_thread.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/write_buffer_manager.h"
#include "test_util/sync_point.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class PoolEvent {
 public:
  void Set() {
    std::lock_guard<std::mutex> lock(mu_);
    ready_ = true;
    cv_.notify_all();
  }
  bool Wait(std::chrono::milliseconds timeout = std::chrono::seconds(20)) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_until(lock, std::chrono::system_clock::now() + timeout,
                          [&] { return ready_; });
  }
  bool IsSet() {
    std::lock_guard<std::mutex> lock(mu_);
    return ready_;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool ready_ = false;
};
}  // namespace

class AsyncCommitExecutorTest : public DBTestBase,
                                public testing::WithParamInterface<int> {
 public:
  AsyncCommitExecutorTest() : DBTestBase("async_commit_executor_test", true) {}
  Options PoolOptions() {
    Options options;
    options.env = env_;
    options.create_if_missing = true;
    options.async_memtable_write = true;
    options.enable_pipelined_write = (GetParam() & 1) != 0;
    options.async_memtable_write_threads = 1 << (GetParam() / 4);
    options.allow_concurrent_memtable_write =
        options.async_memtable_write_threads > 1;
    return options;
  }
  WriteOptions Writes() {
    WriteOptions options;
    options.sync = (GetParam() & 2) != 0;
    options.memtable_insert_hint_per_batch = true;
    return options;
  }
  Status DisableLazyAdvanceSequence(Options* options) {
    DBOptions parsed;
    Status status = GetDBOptionsFromString(
        *options, "lazy_advance_sequence=false;", &parsed);
    if (status.ok()) static_cast<DBOptions&>(*options) = parsed;
    return status;
  }
  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    DBTestBase::TearDown();
  }
  void ExpectDrained() {
    uint64_t groups = 99, bytes = 99;
    EXPECT_TRUE(
        db_->GetIntProperty("rocksdb.async-commit-pending-groups", &groups));
    EXPECT_TRUE(
        db_->GetIntProperty("rocksdb.async-commit-pending-bytes", &bytes));
    EXPECT_EQ(0U, groups);
    EXPECT_EQ(0U, bytes);
  }
};

// Blocking async WAL followers should not construct per-Writer wait objects.
// This exercises a real three-writer group and verifies both final states and
// removal of the old private-CV slow path. Restore BlockingAwaitState's old
// branch to reproduce the two allocation assertions.
TEST(AsyncWalStateWaitTest, SerialFollowersUseSharedBlockingWait) {
  Options options;
  options.async_memtable_write = true;
  options.async_memtable_write_threads = 1;
  options.enable_write_thread_adaptive_yield = false;
  WriteThread queue{ImmutableDBOptions(options)};
  WriteBatch batches[3];
  for (auto& batch : batches) ASSERT_OK(batch.Put("key", "value"));
  WriteOptions writes;
  WriteThread::Writer leader(writes, &batches[0], nullptr, 0, false);
  WriteThread::Writer first(writes, &batches[1], nullptr, 0, false);
  WriteThread::Writer second(writes, &batches[2], nullptr, 0, false);
  PoolEvent blocking;
  std::atomic<unsigned> blocked{0}, resumed{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("WriteThread::AwaitState:BlockingWaiting", [&](void*) {
    if (blocked.fetch_add(1) == 1) blocking.Set();
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:DoneWaiting", [&](void* arg) {
    auto* writer = static_cast<WriteThread::Writer*>(arg);
    EXPECT_FALSE(writer->made_waitable);
    EXPECT_EQ(WriteThread::STATE_WAL_COMPLETED, writer->state.load());
    resumed.fetch_add(1);
  });
  sp->EnableProcessing();
  queue.JoinBatchGroup(&leader);
  std::thread a([&] { queue.JoinBatchGroup(&first); });
  std::thread b([&] { queue.JoinBatchGroup(&second); });
  EXPECT_TRUE(blocking.Wait());
  WriteThread::WriteGroup group;
  queue.EnterAsBatchGroupLeader(&leader, &group);
  EXPECT_EQ(3U, group.size);
  queue.ExitAsyncWalGroup(group, Status::OK());
  a.join();
  b.join();
  EXPECT_EQ(2U, resumed.load());
  EXPECT_OK(first.FinalStatus());
  EXPECT_OK(second.FinalStatus());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// Complete before registration, after registration, and after the predicate
// check. All three transitions must be observed even when no thread was in the
// kernel at the moment of notification. Include terminal WAL errors.
TEST(AsyncWalStateWaitTest, CompletionAcrossBlockingRegistration) {
  for (const char* hook : {"WriteThread::AwaitState:BlockingWaiting",
                           "WriteThread::AsyncWALWait:Registered",
                           "WriteThread::AsyncWALWait:BeforeWait"}) {
    for (bool fail : {false, true}) {
      Options options;
      options.async_memtable_write = true;
      options.async_memtable_write_threads = 1;
      options.enable_write_thread_adaptive_yield = false;
      WriteThread queue{ImmutableDBOptions(options)};
      WriteBatch batch;
      ASSERT_OK(batch.Put("key", "value"));
      WriteOptions writes;
      WriteThread::Writer leader(writes, &batch, nullptr, 0, false);
      WriteThread::Writer follower(writes, &batch, nullptr, 0, false);
      PoolEvent entered, resume;
      auto* sp = SyncPoint::GetInstance();
      sp->SetCallBack(hook, [&](void*) {
        entered.Set();
        EXPECT_TRUE(resume.Wait());
      });
      sp->EnableProcessing();
      queue.JoinBatchGroup(&leader);
      std::thread thread([&] { queue.JoinBatchGroup(&follower); });
      EXPECT_TRUE(entered.Wait());
      WriteThread::WriteGroup group;
      queue.EnterAsBatchGroupLeader(&leader, &group);
      EXPECT_EQ(2U, group.size);
      queue.ExitAsyncWalGroup(group, fail ? Status::IOError("WAL failure")
                                         : Status::OK());
      resume.Set();
      thread.join();
      EXPECT_EQ(fail ? WriteThread::STATE_COMPLETED
                     : WriteThread::STATE_WAL_COMPLETED,
                follower.state.load());
      EXPECT_EQ(fail, follower.FinalStatus().IsIOError());
      sp->DisableProcessing();
      sp->ClearAllCallBacks();
    }
  }
}

// The shared completion wake must not postpone the next leader until after
// old followers are notified. This also exercises harmless wakes of followers
// whose own completion predicate is not ready yet.
TEST(AsyncWalStateWaitTest, NextLeaderRunsBeforeFollowerNotification) {
  Options options;
  options.async_memtable_write = true;
  options.async_memtable_write_threads = 1;
  options.enable_write_thread_adaptive_yield = false;
  WriteThread queue{ImmutableDBOptions(options)};
  WriteBatch batch;
  ASSERT_OK(batch.Put("key", "value"));
  WriteOptions writes;
  WriteThread::Writer leader(writes, &batch, nullptr, 0, false);
  WriteThread::Writer follower(writes, &batch, nullptr, 0, false);
  WriteThread::Writer next(writes, &batch, nullptr, 0, false);
  PoolEvent blocked_follower, blocked_next, next_ready;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("WriteThread::AwaitState:BlockingWaiting", [&](void* arg) {
    if (arg == &follower) blocked_follower.Set();
    if (arg == &next) blocked_next.Set();
  });
  sp->SetCallBack("WriteThread::NotifyAsyncFollowers:Before", [&](void*) {
    EXPECT_TRUE(next_ready.Wait(std::chrono::seconds(1)));
  });
  sp->EnableProcessing();
  queue.JoinBatchGroup(&leader);
  std::thread first([&] { queue.JoinBatchGroup(&follower); });
  EXPECT_TRUE(blocked_follower.Wait());
  WriteThread::WriteGroup group;
  queue.EnterAsBatchGroupLeader(&leader, &group);
  EXPECT_EQ(2U, group.size);
  std::thread second([&] {
    queue.JoinBatchGroup(&next);
    EXPECT_EQ(WriteThread::STATE_GROUP_LEADER, next.state.load());
    next_ready.Set();
  });
  EXPECT_TRUE(blocked_next.Wait());
  queue.ExitAsyncWalGroup(group, Status::OK());
  first.join();
  second.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  queue.EnterAsBatchGroupLeader(&next, &group);
  queue.ExitAsyncWalGroup(group, Status::OK());
}

// A single worker leases the ready backlog even with the multi-worker fetch
// limit set to 1. Readers may observe P before resource retirement; a
// management drain must still wait until the whole batch releases its refs.
TEST_P(AsyncCommitExecutorTest, SerialWorkerPublishesBeforeLeaseFinishes) {
  Options options = PoolOptions();
  options.async_memtable_write_threads = 1;
  options.async_memtable_write_groups_per_fetch = 1;
  options.allow_concurrent_memtable_write = false;
  options.async_memtable_write_wait_spin_rounds = 0;
  DestroyAndReopen(options);
  PoolEvent first, release_first, last, release_last, cleaning, release_batch;
  PoolEvent reader_waiting, read_done, drain_waiting, drain_done;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeRange", [&](void* arg) {
    const auto sequence = *static_cast<SequenceNumber*>(arg);
    if (sequence == 1) {
      first.Set();
      EXPECT_TRUE(release_first.Wait());
    } else if (sequence == 66) {
      last.Set();
      EXPECT_TRUE(release_last.Wait());
    }
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                  [&](void*) { reader_waiting.Set(); });
  sp->SetCallBack("AsyncCommitExecutor::RunSerial:BeforeRelease", [&](void*) {
    if (db_->GetLatestSequenceNumber() == 66) {
      cleaning.Set();
      EXPECT_TRUE(release_batch.Wait());
    }
  });
  sp->SetCallBack("AsyncCommitExecutor::DrainThrough:Wait",
                  [&](void*) { drain_waiting.Set(); });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "key1", "value1"));
  EXPECT_TRUE(first.Wait());
  EXPECT_OK(db_->Put(Writes(), "key2", "value2"));
  std::thread reader([&] {
    ReadOptions read;
    read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
    std::string value;
    EXPECT_OK(db_->Get(read, "key2", &value));
    EXPECT_EQ("value2", value);
    read_done.Set();
  });
  EXPECT_TRUE(reader_waiting.Wait());
  for (unsigned i = 3; i <= 66; ++i) {
    EXPECT_OK(db_->Put(Writes(), "key" + std::to_string(i),
                       "value" + std::to_string(i)));
  }
  release_first.Set();
  EXPECT_TRUE(last.Wait());
  EXPECT_EQ(65U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(read_done.Wait(std::chrono::milliseconds(500)));
  uint64_t leases = 0, pending = 0, drained = 0;
  EXPECT_TRUE(db_->GetIntProperty("rocksdb.async-commit-executor-lease-batches",
                                  &leases));
  EXPECT_TRUE(
      db_->GetIntProperty("rocksdb.async-commit-pending-groups", &pending));
  EXPECT_TRUE(db_->GetIntProperty(
      "rocksdb.async-commit-executor-drained-groups", &drained));
  EXPECT_EQ(2U, leases);
  EXPECT_EQ(65U, pending);
  EXPECT_EQ(1U, drained);

  std::thread drain([&] {
    EXPECT_OK(dbfull()->WaitForAsyncCommit());
    drain_done.Set();
  });
  EXPECT_TRUE(drain_waiting.Wait());
  release_last.Set();
  EXPECT_TRUE(cleaning.Wait(std::chrono::milliseconds(500)));
  EXPECT_EQ(66U, db_->GetLatestSequenceNumber());
  EXPECT_FALSE(drain_done.IsSet());
  release_batch.Set();
  drain.join();
  reader.join();
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ("value66", Get("key66"));
}

TEST_P(AsyncCommitExecutorTest, SerialBatchFailureKeepsPublishedPrefix) {
  Options options = PoolOptions();
  options.async_memtable_write_threads = 1;
  options.allow_concurrent_memtable_write = false;
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);
  PoolEvent first, release_first;
  bool fail_apply = false;
  std::atomic<bool> applied_after_error{false};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    const auto sequence =
        WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg));
    fail_apply = sequence == 3;
    if (sequence == 1) {
      first.Set();
      EXPECT_TRUE(release_first.Wait());
    } else if (sequence == 4) {
      applied_after_error.store(true);
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void* arg) {
    if (fail_apply) {
      *static_cast<Status*>(arg) = Status::IOError("serial batch apply failed");
    }
  });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "one", "v"));
  EXPECT_TRUE(first.Wait());
  EXPECT_OK(db_->Put(Writes(), "two", "v"));
  EXPECT_OK(db_->Put(Writes(), "three", "v"));
  EXPECT_OK(db_->Put(Writes(), "four", "v"));
  release_first.Set();
  EXPECT_TRUE(dbfull()->WaitForAsyncCommit().IsIOError());
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_FALSE(applied_after_error.load());
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_TRUE(db_->Close().IsIOError());
}

// Blocking the notifier must delay multi-worker publication/drain, while the
// single-worker direct publication path remains independent of that thread.
TEST_P(AsyncCommitExecutorTest, NotifierOwnsOnlyMultiWorkerPublication) {
  for (int workers : {1, 2}) {
    Options options = PoolOptions();
    options.async_memtable_write_threads = workers;
    options.allow_concurrent_memtable_write = workers > 1;
    Close();
    PoolEvent idle, resume, inserted, drained;
    std::atomic<bool> blocked{false};
    std::atomic<unsigned> ranges{0};
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack("AsyncCommitSequenceNotifier::Run:BeforeIdleWait",
                    [&](void*) {
                      if (!blocked.exchange(true)) {
                        idle.Set();
                        EXPECT_TRUE(resume.Wait());
                      }
                    });
    sp->SetCallBack("DBImpl::AsyncWriteImpl:RangeComplete", [&](void*) {
      if (ranges.fetch_add(1) == 1) inserted.Set();
    });
    sp->EnableProcessing();
    DestroyAndReopen(options);
    EXPECT_TRUE(idle.Wait());
    EXPECT_OK(db_->Put(Writes(), "first", "one"));
    EXPECT_OK(db_->Put(Writes(), "second", "two"));
    EXPECT_TRUE(inserted.Wait());
    std::thread drain([&] {
      EXPECT_OK(dbfull()->WaitForAsyncCommit());
      drained.Set();
    });
    EXPECT_EQ(workers == 1, drained.Wait(std::chrono::milliseconds(100)));
    if (workers == 2) EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
    resume.Set();
    drain.join();
    EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
    ExpectDrained();
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
  }
}

TEST_P(AsyncCommitExecutorTest, NotifierClosesHoleAcrossMetadataGroup) {
  Options options = PoolOptions();
  options.async_memtable_write_threads =
      std::max(2, options.async_memtable_write_threads);
  options.allow_concurrent_memtable_write = true;
  DestroyAndReopen(options);
  PoolEvent first, release_first, last_received;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 1) {
      first.Set();
      EXPECT_TRUE(release_first.Wait());
    }
  });
  sp->SetCallBack("AsyncCommitSequenceNotifier::ProcessCompleted:Received",
                  [&](void* arg) {
                    if (*static_cast<uint64_t*>(arg) == 4) last_received.Set();
                  });
  sp->EnableProcessing();
  WriteBatch first_batch, metadata, later;
  ASSERT_OK(first_batch.Put("first", "one"));
  ASSERT_OK(first_batch.Put("second", "two"));
  ASSERT_OK(metadata.PutLogData("zero sequence group"));
  ASSERT_OK(later.Put("third", "three"));
  ASSERT_OK(later.Put("fourth", "four"));
  EXPECT_OK(db_->Write(Writes(), &first_batch));
  EXPECT_TRUE(first.Wait());
  EXPECT_OK(db_->Write(Writes(), &metadata));
  EXPECT_OK(db_->Write(Writes(), &later));
  EXPECT_OK(db_->Put(Writes(), "fifth", "five"));
  EXPECT_TRUE(last_received.Wait());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  release_first.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ(5U, db_->GetLatestSequenceNumber());
  uint64_t submitted = 0, drained = 0;
  EXPECT_TRUE(db_->GetIntProperty(
      "rocksdb.async-commit-executor-submitted-groups", &submitted));
  EXPECT_TRUE(db_->GetIntProperty(
      "rocksdb.async-commit-executor-drained-groups", &drained));
  EXPECT_EQ(4U, submitted);
  EXPECT_EQ(4U, drained);
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ("one", Get("first"));
  EXPECT_EQ("five", Get("fifth"));
}

// A large completion inbox must not process newer groups first and postpone
// an already completed prefix until the very last 64-record notification turn.
TEST_P(AsyncCommitExecutorTest, NotifierPublishesBeforeBacklogIsExhausted) {
  Options options = PoolOptions();
  options.async_memtable_write_threads = 2;
  options.allow_concurrent_memtable_write = true;
  Close();
  PoolEvent idle, start, all_reported, next_turn, resume;
  std::array<PoolEvent, 129> reported;
  std::atomic<bool> blocked{false};
  std::atomic<unsigned> received{0};
  static thread_local SequenceNumber applying = 0;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("AsyncCommitSequenceNotifier::Run:BeforeIdleWait",
                  [&](void*) {
                    if (!blocked.exchange(true)) {
                      idle.Set();
                      EXPECT_TRUE(start.Wait());
                    }
                  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    applying = WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg));
    if (applying > 1) EXPECT_TRUE(reported[applying - 1].Wait());
  });
  sp->SetCallBack("AsyncCommitSequenceNotifier::SubmitCompleted:Reported",
                  [&](void*) {
                    reported[applying].Set();
                    if (applying == 128) all_reported.Set();
                  });
  sp->SetCallBack("AsyncCommitSequenceNotifier::ProcessCompleted:Received",
                  [&](void*) {
                    if (received.fetch_add(1) == 64) {
                      next_turn.Set();
                      EXPECT_TRUE(resume.Wait());
                    }
                  });
  sp->EnableProcessing();
  DestroyAndReopen(options);
  EXPECT_TRUE(idle.Wait());
  WriteOptions writes = Writes();
  writes.sync = false;
  for (unsigned i = 1; i <= 128; ++i)
    EXPECT_OK(db_->Put(writes, "key", std::to_string(i)));
  EXPECT_TRUE(all_reported.Wait());
  start.Set();
  EXPECT_TRUE(next_turn.Wait());
  EXPECT_GE(db_->GetLatestSequenceNumber(), 64U);
  resume.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ(128U, db_->GetLatestSequenceNumber());
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ("128", Get("key"));
}

TEST_P(AsyncCommitExecutorTest, OwnedGroupUsesOneQueueNode) {
  Options options = PoolOptions();
  Reopen(options);
  WriteBatch first(0, 0, 8), second(0, 0, 8), metadata(0, 0, 8);
  ASSERT_OK(first.Put("first", "one"));
  ASSERT_OK(WriteBatchInternal::Put(&second, 99999, "missing", "ignored"));
  ASSERT_OK(second.Put("second", "two"));
  metadata.PutLogData("metadata");
  PoolEvent forming, form, followers, applying, apply;
  std::atomic<unsigned> queued{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeGroup", [&](void*) {
    forming.Set();
    EXPECT_TRUE(form.Wait());
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    auto* writer = static_cast<WriteThread::Writer*>(arg);
    if (writer->state.load() != WriteThread::STATE_GROUP_LEADER &&
        queued.fetch_add(1) == 1)
      followers.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(apply.Wait());
  });
  sp->EnableProcessing();
  std::thread leader([&] {
    WriteOptions writes = Writes();
    writes.memtable_insert_hint_per_batch = false;
    EXPECT_OK(db_->Write(writes, &first));
  });
  EXPECT_TRUE(forming.Wait());
  std::thread follower([&] {
    WriteOptions writes = Writes();
    writes.ignore_missing_column_families = true;
    EXPECT_OK(db_->Write(writes, &second));
  });
  std::thread empty([&] { EXPECT_OK(db_->Write(Writes(), &metadata)); });
  EXPECT_TRUE(followers.Wait());
  form.Set();
  EXPECT_TRUE(applying.Wait());
  leader.join();
  follower.join();
  empty.join();
  // Returning callers can reuse all their buffers before the worker inserts.
  first.Clear();
  second.Clear();
  metadata.Clear();
  // This is an outstanding-node gauge, not a cumulative allocation counter.
  // Sample while BeforeApply still pins the worker, before idle reclamation.
  uint64_t retired = 0;
  EXPECT_TRUE(db_->GetIntProperty("rocksdb.async-commit-executor-retired-nodes",
                                 &retired));
  EXPECT_EQ(1U, retired);
  apply.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  uint64_t submitted = 0, ranges = 0;
  EXPECT_TRUE(db_->GetIntProperty("rocksdb.async-commit-executor-submitted-groups",
                                 &submitted));
  EXPECT_TRUE(db_->GetIntProperty("rocksdb.async-commit-executor-lease-ranges",
                                 &ranges));
  EXPECT_EQ(1U, submitted);
  EXPECT_EQ(3U, ranges);
  EXPECT_EQ(3U, db_->GetLatestSequenceNumber());
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ("one", Get("first"));
  EXPECT_EQ("two", Get("second"));
}

// Moving the serial Schedule check into reference retirement must still make
// ordinary foreground writes switch full memtables without an explicit Flush.
TEST_P(AsyncCommitExecutorTest, SerialWorkerSchedulesAutomaticFlush) {
  Options options = PoolOptions();
  options.async_memtable_write_threads = 1;
  options.allow_concurrent_memtable_write = false;
  options.write_buffer_size = 65536;
  options.max_write_buffer_number = 4;
  options.disable_auto_compactions = true;
  Reopen(options);
  for (unsigned i = 0; i < 100; ++i) {
    EXPECT_OK(db_->Put(Writes(), "key" + std::to_string(i),
                       std::string(4096, 'v')));
    if (i % 10 == 9) EXPECT_OK(dbfull()->WaitForAsyncCommit());
  }
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_OK(dbfull()->TEST_WaitForFlushMemTable());
  EXPECT_GT(NumTableFilesAtLevel(0), 0);
  ExpectDrained();
}

// A reader with no preceding write must still wait for writes allocated before
// its entry. Keeping the old thread-local-only target makes this fail.
TEST_P(AsyncCommitExecutorTest, ReadWaitsForOtherThreadsAllocatedSequence) {
  Reopen(PoolOptions());
  PoolEvent applying, release;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->EnableProcessing();
  std::thread writer(
      [&] { EXPECT_OK(db_->Put(Writes(), "pending", "value")); });
  EXPECT_TRUE(applying.Wait());
  writer.join();
  ReadOptions read;
  read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000);
  std::string value;
  EXPECT_TRUE(db_->Get(read, "pending", &value).IsTimedOut());
  release.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ("value", Get("pending"));
}

// An already published snapshot must remain readable while later writes are
// paused. Waiting for the current allocated sequence makes the reader block.
TEST_P(AsyncCommitExecutorTest, SnapshotReadsIgnoreLaterAllocatedSequences) {
  Reopen(PoolOptions());
  ASSERT_OK(db_->Put(Writes(), "existing", "old"));
  const Snapshot* older = db_->GetSnapshot();
  ASSERT_NE(nullptr, older);
  ASSERT_OK(db_->Put(Writes(), "published", "value"));
  const Snapshot* latest = db_->GetSnapshot();
  ASSERT_NE(nullptr, latest);
  ASSERT_EQ(1U, older->GetSequenceNumber());
  ASSERT_EQ(2U, latest->GetSequenceNumber());
  ASSERT_EQ(2U, db_->GetLatestSequenceNumber());

  PoolEvent applying, release, read_done;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->EnableProcessing();
  WriteBatch pending;
  ASSERT_OK(pending.Put("existing", "new"));
  ASSERT_OK(pending.Put("pending", "new"));
  EXPECT_OK(db_->Write(Writes(), &pending));
  EXPECT_TRUE(applying.Wait());

  std::thread reader([&] {
    // Cover both S < P and S == P, with an unpublished newer write group.
    for (const Snapshot* snapshot : {older, latest}) {
      SCOPED_TRACE(snapshot->GetSequenceNumber());
      ReadOptions read;
      read.snapshot = snapshot;
      std::string value;
      EXPECT_OK(db_->Get(read, "existing", &value));
      EXPECT_EQ("old", value);
      EXPECT_TRUE(db_->Get(read, "pending", &value).IsNotFound());

      const Slice keys[] = {Slice("existing"), Slice("pending")};
      std::vector<std::string> values;
      auto statuses = db_->MultiGet(read, {keys[0], keys[1]}, &values);
      EXPECT_OK(statuses[0]);
      EXPECT_EQ("old", values[0]);
      EXPECT_TRUE(statuses[1].IsNotFound());

      PinnableSlice pinned[2];
      Status batched_status[2];
      db_->MultiGet(read, db_->DefaultColumnFamily(), 2, keys, pinned,
                    batched_status);
      EXPECT_OK(batched_status[0]);
      EXPECT_EQ("old", pinned[0].ToString());
      EXPECT_TRUE(batched_status[1].IsNotFound());

      bool found = false;
      EXPECT_TRUE(db_->KeyMayExist(read, db_->DefaultColumnFamily(), "existing",
                                   &value, &found));
      EXPECT_TRUE(found);
      EXPECT_EQ("old", value);

      std::unique_ptr<Iterator> iter(db_->NewIterator(read));
      iter->Seek("existing");
      EXPECT_OK(iter->status());
      EXPECT_TRUE(iter->Valid());
      if (iter->Valid()) {
        EXPECT_EQ("old", iter->value().ToString());
      }
      std::vector<Iterator*> iterators;
      EXPECT_OK(
          db_->NewIterators(read, {db_->DefaultColumnFamily()}, &iterators));
      EXPECT_EQ(1U, iterators.size());
      for (auto* iterator : iterators) {
        iterator->Seek("existing");
        EXPECT_OK(iterator->status());
        EXPECT_TRUE(iterator->Valid());
        if (iterator->Valid()) {
          EXPECT_EQ("old", iterator->value().ToString());
        }
        delete iterator;
      }
    }
    read_done.Set();
  });
  EXPECT_TRUE(read_done.Wait(std::chrono::seconds(1)));
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  release.Set();
  reader.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ("new", Get("existing"));
  db_->ReleaseSnapshot(older);
  db_->ReleaseSnapshot(latest);
  ExpectDrained();
}

// Capturing A only after WAL success would let this reader miss an in-flight
// allocation. A WAL failure must instead wake it with the terminal error.
TEST_P(AsyncCommitExecutorTest, ReadCapturesSequenceBeforeWALCompletes) {
  Reopen(PoolOptions());
  PoolEvent allocated, release;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:SequenceAllocated", [&](void*) {
    allocated.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALStatus", [&](void* arg) {
    *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected WAL failure");
  });
  sp->EnableProcessing();
  std::thread writer(
      [&] { EXPECT_TRUE(db_->Put(Writes(), "pending", "value").IsIOError()); });
  EXPECT_TRUE(allocated.Wait());
  ReadOptions read;
  read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000);
  std::string value;
  EXPECT_TRUE(db_->Get(read, "pending", &value).IsTimedOut());
  release.Set();
  writer.join();
  EXPECT_TRUE(db_->Get(ReadOptions(), "pending", &value).IsIOError());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// Reading A again after the entry hook would incorrectly wait for sequence 2.
TEST_P(AsyncCommitExecutorTest, ReadTargetDoesNotChaseLaterAllocations) {
  Reopen(PoolOptions());
  PoolEvent first, second, release_first, release_second, captured, resume;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 1) {
      first.Set();
      EXPECT_TRUE(release_first.Wait());
    } else {
      second.Set();
      EXPECT_TRUE(release_second.Wait());
    }
  });
  sp->SetCallBack("DBImpl::PrepareAsyncCommitRead:Wait", [&](void* arg) {
    EXPECT_EQ(1U, *static_cast<SequenceNumber*>(arg));
    captured.Set();
    EXPECT_TRUE(resume.Wait());
  });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "first", "one"));
  EXPECT_TRUE(first.Wait());
  std::thread reader([&] {
    ReadOptions read;
    read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
    std::string value;
    EXPECT_OK(db_->Get(read, "first", &value));
    EXPECT_EQ("one", value);
  });
  EXPECT_TRUE(captured.Wait());
  EXPECT_OK(db_->Put(Writes(), "second", "two"));
  release_first.Set();
  EXPECT_TRUE(second.Wait());
  // With multiple workers the second task may start before P reaches 1.
  PoolEvent published;
  sp->SetCallBack("DBImpl::SignalAsyncCommitPublication:Published",
                  [&](void*) { published.Set(); });
  if (db_->GetLatestSequenceNumber() < 1) {
    EXPECT_TRUE(published.Wait());
  }
  resume.Set();
  reader.join();
  release_second.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// Slow reader wakeups must not prevent subsequent memtable publication.
TEST_P(AsyncCommitExecutorTest,
       WorkerContinuesWhilePublicationNotificationIsBlocked) {
  Options options = PoolOptions();
  options.async_memtable_write_wait_spin_rounds = 0;
  Reopen(options);
  PoolEvent applying, release_apply, waiting, notifying, release_notify;
  PoolEvent next_applied;
  std::atomic<unsigned> applies{0};
  std::atomic<bool> blocked{false};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    if (applies.fetch_add(1) == 0) {
      applying.Set();
      EXPECT_TRUE(release_apply.Wait());
    } else {
      next_applied.Set();
    }
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                  [&](void*) { waiting.Set(); });
  sp->SetCallBack("DBImpl::NotifyAsyncCommitPublication:BeforeSlotLock",
                  [&](void*) {
                    if (!blocked.exchange(true)) {
                      notifying.Set();
                      EXPECT_TRUE(release_notify.Wait());
                    }
                  });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "first", "value"));
  EXPECT_TRUE(applying.Wait());
  std::thread reader([&] {
    ReadOptions read;
    read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
    std::string value;
    EXPECT_OK(db_->Get(read, "first", &value));
    EXPECT_EQ("value", value);
  });
  EXPECT_TRUE(waiting.Wait());
  release_apply.Set();
  EXPECT_TRUE(notifying.Wait());
  EXPECT_OK(db_->Put(Writes(), "second", "next"));
  // In the old implementation a single worker is stuck in the first group's
  // notification, so this real second insertion cannot start.
  EXPECT_TRUE(next_applied.Wait(std::chrono::milliseconds(500)));
  release_notify.Set();
  reader.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("next", Get("second"));
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// The last publication can occur after the epoch snapshot or immediately
// before the actual futex wait. No later Put may be needed to wake the reader.
TEST_P(AsyncCommitExecutorTest, NotifierProgressBeforeSleepIsNotLost) {
  for (const char* point :
       {"AsyncCommitSequenceNotifier::Run:ProgressRegistered",
        "AsyncCommitSequenceNotifier::Run:BeforeProgressWait"}) {
    Options options = PoolOptions();
    options.async_memtable_write_wait_spin_rounds = 0;
    Reopen(options);
    PoolEvent applying, release_apply, registered, resume, read_done;
    PoolEvent reported;
    std::atomic<bool> blocked{false};
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
      applying.Set();
      EXPECT_TRUE(release_apply.Wait());
    });
    sp->SetCallBack(point, [&](void*) {
      if (!blocked.exchange(true)) {
        registered.Set();
        EXPECT_TRUE(resume.Wait());
      }
    });
    sp->SetCallBack("AsyncCommitSequenceNotifier::SubmitCompleted:Reported",
                    [&](void*) { reported.Set(); });
    sp->EnableProcessing();
    EXPECT_OK(db_->Put(Writes(), "key", "value"));
    EXPECT_TRUE(applying.Wait());
    std::thread reader([&] {
      ReadOptions read;
      read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
      std::string value;
      EXPECT_OK(db_->Get(read, "key", &value));
      EXPECT_EQ("value", value);
      read_done.Set();
    });
    EXPECT_TRUE(registered.Wait());
    release_apply.Set();
    if (options.async_memtable_write_threads > 1) {
      EXPECT_TRUE(reported.Wait());
      resume.Set();
    }
    EXPECT_OK(dbfull()->WaitForAsyncCommit());
    resume.Set();
    EXPECT_TRUE(read_done.Wait(std::chrono::milliseconds(500)));
    reader.join();
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
  }
}

// A first waiter must activate the notifier even if P advanced after the
// notifier's zero-waiter observation. Skipping that interval loses this wakeup.
TEST_P(AsyncCommitExecutorTest, FirstWaiterActivatesIdleNotifier) {
  for (const char* point :
       {"AsyncCommitSequenceNotifier::Run:IdleRegistered",
        "AsyncCommitSequenceNotifier::Run:BeforeIdleWait"}) {
    Options options = PoolOptions();
    options.async_memtable_write_wait_spin_rounds = 0;
    Close();
    PoolEvent idle, resume, applying, release_apply, waiting, read_done;
    PoolEvent reported;
    std::atomic<bool> blocked{false};
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack(point, [&](void*) {
      if (!blocked.exchange(true)) {
        idle.Set();
        EXPECT_TRUE(resume.Wait());
      }
    });
    sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
      applying.Set();
      EXPECT_TRUE(release_apply.Wait());
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                    [&](void*) { waiting.Set(); });
    sp->SetCallBack("AsyncCommitSequenceNotifier::SubmitCompleted:Reported",
                    [&](void*) { reported.Set(); });
    sp->EnableProcessing();
    Reopen(options);
    EXPECT_TRUE(idle.Wait());
    EXPECT_OK(db_->Put(Writes(), "key", "value"));
    EXPECT_TRUE(applying.Wait());
    std::thread reader([&] {
      ReadOptions read;
      read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
      std::string value;
      EXPECT_OK(db_->Get(read, "key", &value));
      EXPECT_EQ("value", value);
      read_done.Set();
    });
    EXPECT_TRUE(waiting.Wait());
    release_apply.Set();
    if (options.async_memtable_write_threads > 1) {
      EXPECT_TRUE(reported.Wait());
      resume.Set();
    }
    EXPECT_OK(dbfull()->WaitForAsyncCommit());
    resume.Set();
    EXPECT_TRUE(read_done.Wait(std::chrono::milliseconds(500)));
    reader.join();
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
  }
}

// P advances by a full slot cycle while the notifier services its old snapshot.
// Advancing N to a fresh P at the end, or treating equal modulo slots as an
// empty interval, leaves the reader of sequence 2 asleep.
TEST_P(AsyncCommitExecutorTest, NotifierKeepsSnapshotAcrossSlotCycle) {
  Options options = PoolOptions();
  // Only a direct publisher can advance P while this notifier is blocked.
  options.async_memtable_write_threads = 1;
  options.allow_concurrent_memtable_write = false;
  options.async_memtable_write_wait_slots = 3;
  options.async_memtable_write_wait_spin_rounds = 0;
  DestroyAndReopen(options);
  PoolEvent first_apply, release_first, second_apply, release_second;
  PoolEvent first_wait, second_wait, notifying, resume, second_done;
  std::atomic<bool> blocked{false};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    const auto sequence =
        WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg));
    if (sequence == 1) {
      first_apply.Set();
      EXPECT_TRUE(release_first.Wait());
    } else if (sequence == 2) {
      second_apply.Set();
      EXPECT_TRUE(release_second.Wait());
    }
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                  [&](void* arg) {
                    if (*static_cast<uint32_t*>(arg) == 1) first_wait.Set();
                    if (*static_cast<uint32_t*>(arg) == 2) second_wait.Set();
                  });
  sp->SetCallBack("DBImpl::NotifyAsyncCommitPublication:BeforeSlotLock",
                  [&](void*) {
                    if (!blocked.exchange(true)) {
                      notifying.Set();
                      EXPECT_TRUE(resume.Wait());
                    }
                  });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "first", "one"));
  EXPECT_TRUE(first_apply.Wait());
  auto read = [&](const char* key, const char* expected) {
    ReadOptions ro;
    ro.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
    std::string value;
    EXPECT_OK(db_->Get(ro, key, &value));
    EXPECT_EQ(expected, value);
  };
  std::thread first([&] { read("first", "one"); });
  EXPECT_TRUE(first_wait.Wait());
  release_first.Set();
  EXPECT_TRUE(notifying.Wait());
  EXPECT_OK(db_->Put(Writes(), "second", "two"));
  EXPECT_TRUE(second_apply.Wait());
  std::thread second([&] {
    read("second", "two");
    second_done.Set();
  });
  EXPECT_TRUE(second_wait.Wait());
  release_second.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  WriteBatch batch;
  EXPECT_OK(batch.Put("third", "three"));
  EXPECT_OK(batch.Put("fourth", "four"));
  EXPECT_OK(db_->Write(Writes(), &batch));
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ(4U, db_->GetLatestSequenceNumber());
  resume.Set();
  EXPECT_TRUE(second_done.Wait(std::chrono::milliseconds(500)));
  first.join();
  second.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

TEST_P(AsyncCommitExecutorTest,
       WeakWritesLeaveNotifierIdleAndTimeoutReturnsToIdle) {
  Options options = PoolOptions();
  options.async_memtable_write_wait_spin_rounds = 0;
  Close();
  PoolEvent idle, idle_again, applying, release_apply;
  std::atomic<unsigned> idles{0}, scans{0}, progress_waits{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("AsyncCommitSequenceNotifier::Run:IdleRegistered",
                  [&](void*) {
                    if (idles.fetch_add(1) == 0)
                      idle.Set();
                    else
                      idle_again.Set();
                  });
  sp->SetCallBack("DBImpl::NotifyAsyncCommitPublication:BeforeSlotLock",
                  [&](void*) { ++scans; });
  sp->SetCallBack("AsyncCommitSequenceNotifier::Run:ProgressRegistered",
                  [&](void*) { ++progress_waits; });
  sp->EnableProcessing();
  Reopen(options);
  EXPECT_TRUE(idle.Wait());
  for (unsigned i = 0; i < 16; ++i)
    EXPECT_OK(db_->Put(Writes(), "key", "value"));
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ(0U, scans.load());
  EXPECT_EQ(0U, progress_waits.load());
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release_apply.Wait());
  });
  EXPECT_OK(db_->Put(Writes(), "pending", "next"));
  EXPECT_TRUE(applying.Wait());
  ReadOptions ro;
  ro.deadline = std::chrono::microseconds(env_->NowMicros() + 10000);
  std::string value;
  EXPECT_TRUE(db_->Get(ro, "pending", &value).IsTimedOut());
  EXPECT_TRUE(idle_again.Wait(std::chrono::milliseconds(500)));
  release_apply.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ("next", Get("pending"));
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// Exercise both event sleep locations with the real Stop/Join protocol. There
// need not be another P advance or waiter transition to terminate the thread.
TEST_P(AsyncCommitExecutorTest, NotifierStopWakesBothEvents) {
  Reopen(PoolOptions());
  for (bool waiting : {false, true}) {
    AsyncCommitSequenceNotifier notifier(dbfull());
    if (waiting) notifier.RegisterWaiter();
    PoolEvent sleeping, stopped;
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack(waiting
                        ? "AsyncCommitSequenceNotifier::Run:BeforeProgressWait"
                        : "AsyncCommitSequenceNotifier::Run:BeforeIdleWait",
                    [&](void* arg) {
                      if (arg == &notifier) sleeping.Set();
                    });
    sp->EnableProcessing();
    notifier.Start(db_->GetLatestSequenceNumber());
    EXPECT_TRUE(sleeping.Wait());
    std::thread stop([&] {
      notifier.Stop();
      stopped.Set();
    });
    EXPECT_TRUE(stopped.Wait(std::chrono::milliseconds(500)));
    // Ensure cleanup even if Stop fails to signal one of its events.
    if (waiting)
      notifier.PublicationAdvanced();
    else {
      notifier.RegisterWaiter();
      notifier.UnregisterWaiter();
    }
    stop.join();
    if (waiting) notifier.UnregisterWaiter();
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
  }
}

TEST_P(AsyncCommitExecutorTest, WorkerZeroDoesNotWaitForReaderNotification) {
  Options options = PoolOptions();
  options.async_memtable_write_threads = 0;
  options.async_memtable_write_wait_spin_rounds = 0;
  Reopen(options);
  PoolEvent applying, release_apply, waiting, notifying, resume, writer_done;
  std::atomic<bool> blocked{false};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release_apply.Wait());
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                  [&](void*) { waiting.Set(); });
  sp->SetCallBack("DBImpl::NotifyAsyncCommitPublication:BeforeSlotLock",
                  [&](void*) {
                    if (!blocked.exchange(true)) {
                      notifying.Set();
                      EXPECT_TRUE(resume.Wait());
                    }
                  });
  sp->EnableProcessing();
  std::thread writer([&] {
    EXPECT_OK(db_->Put(Writes(), "key", "value"));
    writer_done.Set();
  });
  EXPECT_TRUE(applying.Wait());
  std::thread reader([&] {
    ReadOptions ro;
    ro.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
    std::string value;
    EXPECT_OK(db_->Get(ro, "key", &value));
    EXPECT_EQ("value", value);
  });
  EXPECT_TRUE(waiting.Wait());
  release_apply.Set();
  EXPECT_TRUE(notifying.Wait());
  EXPECT_TRUE(writer_done.Wait(std::chrono::milliseconds(500)));
  resume.Set();
  writer.join();
  reader.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// Publishing between a reader's unlocked check and its slot lock must not
// strand it, even when the publisher sees no registered waiters.
TEST_P(AsyncCommitExecutorTest, PublicationBeforeSlotLockIsNotLost) {
  Options options = PoolOptions();
  options.async_memtable_write_wait_spin_rounds = 0;
  Reopen(options);
  PoolEvent applying, release, before_lock, resume;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeSlotLock",
                  [&](void*) {
                    before_lock.Set();
                    EXPECT_TRUE(resume.Wait());
                  });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "key", "value"));
  EXPECT_TRUE(applying.Wait());
  std::thread reader([&] {
    ReadOptions read;
    read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
    std::string value;
    EXPECT_OK(db_->Get(read, "key", &value));
    EXPECT_EQ("value", value);
  });
  EXPECT_TRUE(before_lock.Wait());
  release.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  resume.Set();
  reader.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// The publisher must synchronize with the slot mutex after setting P. Merely
// loading a waiter count without a handshake loses this notification.
TEST_P(AsyncCommitExecutorTest, PublicationBetweenPredicateAndSleepIsNotLost) {
  Options options = PoolOptions();
  options.async_memtable_write_wait_spin_rounds = 0;
  Reopen(options);
  PoolEvent applying, release, before_wait, resume, notifying;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait", [&](void*) {
    before_wait.Set();
    EXPECT_TRUE(resume.Wait());
  });
  sp->SetCallBack("DBImpl::NotifyAsyncCommitPublication:BeforeSlotLock",
                  [&](void*) { notifying.Set(); });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "key", "value"));
  EXPECT_TRUE(applying.Wait());
  std::thread reader([&] {
    ReadOptions read;
    read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
    std::string value;
    EXPECT_OK(db_->Get(read, "key", &value));
    EXPECT_EQ("value", value);
  });
  EXPECT_TRUE(before_wait.Wait());
  release.Set();
  EXPECT_TRUE(notifying.Wait());
  resume.Set();
  reader.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// A publication/error during a delay is observed at the next outer poll.
// This distinguishes grouped delay from checking P after every PAUSE.
TEST_P(AsyncCommitExecutorTest, RandomSpinDelayGroupsPauses) {
  for (bool fail : {false, true}) {
    Options options = PoolOptions();
    DBOptions parsed;
    ASSERT_OK(GetDBOptionsFromString(
        options,
        "async_memtable_write_wait_spin_rounds=4294967295;"
        "async_memtable_write_wait_spin_delay=2;"
        "async_memtable_write_wait_spin_pause_multiplier=4;",
        &parsed));
    static_cast<DBOptions&>(options) = parsed;
    DestroyAndReopen(options);
    PoolEvent applying, release;
    unsigned rounds = 0, pauses = 0, blocked = 0;
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    });
    if (fail) {
      sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("spin delay apply failure");
      });
    }
    sp->SetCallBack("DBImpl::PrepareAsyncCommitRead:Wait", [&](void*) {
      Random::GetTLSInstance()->Reset(1);  // First Uniform(2) is 1.
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Spin", [&](void* arg) {
      ++rounds;
      EXPECT_EQ(4U, *static_cast<uint32_t*>(arg));
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Pause", [&](void*) {
      if (++pauses == 1) {
        release.Set();
        Status status = dbfull()->WaitForAsyncCommit();
        if (fail) EXPECT_TRUE(status.IsIOError());
        else EXPECT_OK(status);
      }
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                    [&](void*) { ++blocked; });
    sp->EnableProcessing();
    EXPECT_OK(db_->Put(Writes(), "key", "value"));
    EXPECT_TRUE(applying.Wait());
    std::string value;
    Status status = db_->Get(ReadOptions(), "key", &value);
    if (fail) EXPECT_TRUE(status.IsIOError());
    else {
      EXPECT_OK(status);
      EXPECT_EQ("value", value);
    }
    EXPECT_EQ(1U, rounds);
    EXPECT_EQ(4U, pauses);
    EXPECT_EQ(0U, blocked);
    release.Set();
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
    if (fail) EXPECT_TRUE(db_->Close().IsIOError());
  }
}

TEST_P(AsyncCommitExecutorTest, RandomSpinDelayRangeAndZeroSettings) {
  for (const auto& setting : {std::make_pair(6U, 50U),
                              std::make_pair(0U, 50U),
                              std::make_pair(1U, 50U),
                              std::make_pair(6U, 0U)}) {
    Options options = PoolOptions();
    DBOptions parsed;
    ASSERT_OK(GetDBOptionsFromString(
        options,
        "async_memtable_write_wait_spin_rounds=3;async_memtable_write_wait_"
        "spin_delay=" +
            std::to_string(setting.first) +
            ";async_memtable_write_wait_spin_pause_multiplier=" +
            std::to_string(setting.second),
        &parsed));
    static_cast<DBOptions&>(options) = parsed;
    Reopen(options);
    PoolEvent applying, release;
    Random expected(1);
    unsigned rounds = 0, pauses = 0, expected_total = 0;
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    });
    sp->SetCallBack("DBImpl::PrepareAsyncCommitRead:Wait", [&](void*) {
      Random::GetTLSInstance()->Reset(1);
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Spin", [&](void* arg) {
      EXPECT_EQ(expected_total, pauses);  // Previous delay actually finished.
      ++rounds;
      const unsigned n = setting.first && setting.second
          ? expected.Uniform(setting.first) * setting.second : 0;
      EXPECT_EQ(n, *static_cast<uint32_t*>(arg));
      expected_total += n;
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Pause",
                    [&](void*) { ++pauses; });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait", [&](void*) {
      EXPECT_EQ(3U, rounds);
      EXPECT_EQ(expected_total, pauses);
      release.Set();
    });
    sp->EnableProcessing();
    EXPECT_OK(db_->Put(Writes(), "key", "value"));
    EXPECT_TRUE(applying.Wait());
    std::string value;
    EXPECT_OK(db_->Get(ReadOptions(), "key", &value));
    EXPECT_EQ("value", value);
    EXPECT_EQ(3U, rounds);
    EXPECT_EQ(expected_total, pauses);
    release.Set();
    EXPECT_OK(dbfull()->WaitForAsyncCommit());
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
  }
}

TEST_P(AsyncCommitExecutorTest, RandomSpinDelayDeadlineInterruptsInnerLoop) {
  Options options = PoolOptions();
  DBOptions parsed;
  ASSERT_OK(GetDBOptionsFromString(
      options,
      "async_memtable_write_wait_spin_rounds=4294967295;"
      "async_memtable_write_wait_spin_delay=1000;"
      "async_memtable_write_wait_spin_pause_multiplier=100;",
      &parsed));
  static_cast<DBOptions&>(options) = parsed;
  SetTimeElapseOnlySleepOnReopen(&options);
  Reopen(options);
  PoolEvent applying, release;
  unsigned rounds = 0, pauses = 0, blocked = 0;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("DBImpl::PrepareAsyncCommitRead:Wait", [&](void*) {
    Random::GetTLSInstance()->Reset(1);  // Delay is 80700 PAUSEs.
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Spin",
                  [&](void*) { ++rounds; });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Pause", [&](void*) {
    if (++pauses == 1) env_->MockSleepForMicroseconds(10000000);
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                  [&](void*) { ++blocked; });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "key", "value"));
  EXPECT_TRUE(applying.Wait());
  ReadOptions read;
  read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000000);
  std::string value;
  EXPECT_TRUE(db_->Get(read, "key", &value).IsTimedOut());
  EXPECT_EQ(1U, rounds);
  EXPECT_EQ(64U, pauses);
  EXPECT_EQ(0U, blocked);
  release.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

TEST_P(AsyncCommitExecutorTest, CountedSpinCanFinishWithoutBlocking) {
  for (uint32_t rounds : {1U, 64U, 200U}) {
    for (uint32_t publish_at : {1U, rounds}) {
      Options options = PoolOptions();
      DBOptions parsed;
      ASSERT_OK(GetDBOptionsFromString(
          options,
          "async_memtable_write_wait_spin_rounds=" + std::to_string(rounds),
          &parsed));
      static_cast<DBOptions&>(options) = parsed;
      Reopen(options);
      PoolEvent applying, release;
      unsigned busy = 0, blocked = 0;
      auto* sp = SyncPoint::GetInstance();
      sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
        applying.Set();
        EXPECT_TRUE(release.Wait());
      });
      sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Spin", [&](void*) {
        if (++busy == publish_at) {
          release.Set();
          // Publication is complete before this iteration's pause finishes.
          EXPECT_OK(dbfull()->WaitForAsyncCommit());
        }
      });
      sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                      [&](void*) { ++blocked; });
      sp->EnableProcessing();
      EXPECT_OK(db_->Put(Writes(), "key", "value"));
      EXPECT_TRUE(applying.Wait());
      std::string value;
      EXPECT_OK(db_->Get(ReadOptions(), "key", &value));
      EXPECT_EQ("value", value);
      EXPECT_EQ(publish_at, busy);
      EXPECT_EQ(0U, blocked);
      release.Set();
      EXPECT_OK(dbfull()->WaitForAsyncCommit());
      sp->DisableProcessing();
      sp->ClearAllCallBacks();
    }
  }
}

TEST_P(AsyncCommitExecutorTest, CountedSpinExhaustionEntersCV) {
  for (uint32_t rounds : {0U, 1U, 64U, 200U, 1000U}) {
    Options options = PoolOptions();
    DBOptions parsed;
    ASSERT_OK(GetDBOptionsFromString(
        options,
        "async_memtable_write_wait_spin_rounds=" + std::to_string(rounds),
        &parsed));
    static_cast<DBOptions&>(options) = parsed;
    Reopen(options);
    PoolEvent applying, release;
    unsigned busy = 0, blocked = 0;
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Spin",
                    [&](void*) { ++busy; });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait", [&](void*) {
      ++blocked;
      EXPECT_EQ(rounds, busy);
      release.Set();
    });
    sp->EnableProcessing();
    EXPECT_OK(db_->Put(Writes(), "key", "value"));
    EXPECT_TRUE(applying.Wait());
    std::string value;
    EXPECT_OK(db_->Get(ReadOptions(), "key", &value));
    EXPECT_EQ("value", value);
    EXPECT_EQ(rounds, busy);
    EXPECT_GT(blocked, 0U);
    release.Set();
    EXPECT_OK(dbfull()->WaitForAsyncCommit());
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
  }
}

TEST_P(AsyncCommitExecutorTest, CountedSpinHonorsDeadlineAtBoundaries) {
  // Expire the DB clock during the spin. A large budget must not defer the
  // deadline until it is exhausted, including when no spin is configured.
  for (uint32_t rounds : {0U, 1U, 64U, 65U, 0xffffffffU}) {
    Options options = PoolOptions();
    options.async_memtable_write_wait_spin_delay = 0;
    DBOptions parsed;
    ASSERT_OK(GetDBOptionsFromString(
        options,
        "async_memtable_write_wait_spin_rounds=" + std::to_string(rounds),
        &parsed));
    static_cast<DBOptions&>(options) = parsed;
    SetTimeElapseOnlySleepOnReopen(&options);
    Reopen(options);
    PoolEvent applying, release;
    unsigned busy = 0, blocked = 0;
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:Spin", [&](void*) {
      if (++busy == 1) env_->MockSleepForMicroseconds(10000000);
    });
    sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                    [&](void*) { ++blocked; });
    sp->EnableProcessing();
    EXPECT_OK(db_->Put(Writes(), "key", "value"));
    EXPECT_TRUE(applying.Wait());
    ReadOptions read;
    read.deadline = std::chrono::microseconds(
        rounds == 0 ? -1 : env_->NowMicros() + 5000000);
    std::string value;
    EXPECT_TRUE(db_->Get(read, "key", &value).IsTimedOut());
    EXPECT_EQ(std::min(rounds, 64U), busy);
    EXPECT_EQ(0U, blocked);
    release.Set();
    EXPECT_OK(dbfull()->WaitForAsyncCommit());
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
  }
}

TEST_P(AsyncCommitExecutorTest, CountedSpinCVHonorsDeadline) {
  Options options = PoolOptions();
  options.async_memtable_write_wait_spin_rounds = 0;
  Reopen(options);
  PoolEvent applying, release;
  unsigned blocked = 0;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                  [&](void*) { ++blocked; });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "key", "value"));
  EXPECT_TRUE(applying.Wait());
  ReadOptions read;
  read.deadline = std::chrono::microseconds(env_->NowMicros() + 5000);
  std::string value;
  EXPECT_TRUE(db_->Get(read, "key", &value).IsTimedOut());
  EXPECT_GT(blocked, 0U);
  release.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

// Targets 1..6 cover separate slots and collisions. Replacing notify_all with
// notify_one, waking only P % N, or failing to broadcast errors strands
// readers.
TEST_P(AsyncCommitExecutorTest, PublicationAndErrorsWakeAllCoveredSlots) {
  for (uint32_t slots : {1U, 3U, 32U, 1024U}) {
    for (bool fail : {false, true}) {
      Options options = PoolOptions();
      options.async_memtable_write_wait_slots = slots;
      options.async_memtable_write_wait_spin_rounds = 0;
      DestroyAndReopen(options);
      PoolEvent applying, release;
      std::vector<std::unique_ptr<PoolEvent>> waiting;
      for (int i = 0; i < 6; ++i) waiting.emplace_back(new PoolEvent);
      static thread_local size_t reader_id;
      static thread_local bool fail_range;
      auto* sp = SyncPoint::GetInstance();
      sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
        fail_range =
            WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 1;
        if (fail_range) {
          applying.Set();
          EXPECT_TRUE(release.Wait());
        }
      });
      if (fail) {
        sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void* arg) {
          if (fail_range)
            *static_cast<Status*>(arg) = Status::IOError("publication failed");
        });
      }
      sp->SetCallBack("DBImpl::WaitForAsyncCommitSequence:BeforeWait",
                      [&](void* arg) {
                        const uint32_t by_three[] = {1, 2, 0, 1, 2, 0};
                        const uint32_t expected =
                            slots == 1   ? 0
                            : slots == 3 ? by_three[reader_id]
                                         : static_cast<uint32_t>(reader_id + 1);
                        EXPECT_EQ(expected, *static_cast<uint32_t*>(arg));
                        waiting[reader_id]->Set();
                      });
      sp->EnableProcessing();
      std::vector<std::thread> readers;
      for (size_t i = 0; i < 6; ++i) {
        EXPECT_OK(db_->Put(Writes(), std::to_string(i), "value"));
        if (i == 0) {
          EXPECT_TRUE(applying.Wait());
        }
        readers.emplace_back([&, i] {
          reader_id = i;
          ReadOptions read;
          read.deadline =
              std::chrono::microseconds(env_->NowMicros() + 5000000);
          std::string value;
          Status status = db_->Get(read, std::to_string(i), &value);
          if (fail) {
            EXPECT_TRUE(status.IsIOError());
          } else {
            EXPECT_OK(status);
            EXPECT_EQ("value", value);
          }
        });
        EXPECT_TRUE(waiting[i]->Wait());
      }
      release.Set();
      for (auto& reader : readers) reader.join();
      Status drained = dbfull()->WaitForAsyncCommit();
      if (fail) {
        EXPECT_TRUE(drained.IsIOError());
      } else {
        EXPECT_OK(drained);
      }
      sp->DisableProcessing();
      sp->ClearAllCallBacks();
      ExpectDrained();
    }
  }
}

TEST_P(AsyncCommitExecutorTest, EagerSequenceWaitsForLeaderAndFollowers) {
  Options options = PoolOptions();
  ASSERT_OK(DisableLazyAdvanceSequence(&options));
  Reopen(options);
  PoolEvent forming, form, follower_queued, applying, release, next_wal;
  PoolEvent leader_ack, follower_ack, next_ack;
  std::atomic<bool> first_group{true};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeGroup", [&](void*) {
    if (first_group.exchange(false)) {
      forming.Set();
      EXPECT_TRUE(form.Wait());
    }
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    if (static_cast<WriteThread::Writer*>(arg)->state.load() !=
        WriteThread::STATE_GROUP_LEADER)
      follower_queued.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeRange", [&](void* arg) {
    if (*static_cast<SequenceNumber*>(arg) == 1) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 3)
      next_wal.Set();
  });
  sp->EnableProcessing();
  std::thread leader([&] {
    EXPECT_OK(db_->Put(Writes(), "first", "one"));
    EXPECT_GE(db_->GetLatestSequenceNumber(), 2U);
    leader_ack.Set();
  });
  EXPECT_TRUE(forming.Wait());
  std::thread follower([&] {
    EXPECT_OK(db_->Put(Writes(), "second", "two"));
    EXPECT_GE(db_->GetLatestSequenceNumber(), 2U);
    follower_ack.Set();
  });
  EXPECT_TRUE(follower_queued.Wait());
  form.Set();
  EXPECT_TRUE(applying.Wait());
  std::thread next([&] {
    EXPECT_OK(db_->Put(Writes(), "third", "three"));
    EXPECT_GE(db_->GetLatestSequenceNumber(), 3U);
    next_ack.Set();
  });
  // Waiting callers must release the WAL gate before publication.
  EXPECT_TRUE(next_wal.Wait());
  EXPECT_FALSE(leader_ack.Wait(std::chrono::milliseconds(30)));
  EXPECT_FALSE(follower_ack.Wait(std::chrono::milliseconds(30)));
  EXPECT_FALSE(next_ack.Wait(std::chrono::milliseconds(30)));
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  release.Set();
  leader.join();
  follower.join();
  next.join();
  EXPECT_EQ("one", Get("first"));
  EXPECT_EQ("two", Get("second"));
  EXPECT_EQ("three", Get("third"));
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, EagerSequenceWaitsForEarlierGroupPublication) {
  Options options = PoolOptions();
  if (options.async_memtable_write_threads < 2) return;
  ASSERT_OK(DisableLazyAdvanceSequence(&options));
  Reopen(options);
  PoolEvent applying, release, later_applied, first_ack, later_ack;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeRange", [&](void* arg) {
    if (*static_cast<SequenceNumber*>(arg) == 1) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:RangeComplete", [&](void* arg) {
    if (*static_cast<SequenceNumber*>(arg) == 2) later_applied.Set();
  });
  sp->EnableProcessing();
  std::thread first([&] {
    EXPECT_OK(db_->Put(Writes(), "first", "one"));
    first_ack.Set();
  });
  EXPECT_TRUE(applying.Wait());
  std::thread later([&] {
    EXPECT_OK(db_->Put(Writes(), "later", "two"));
    later_ack.Set();
  });
  EXPECT_TRUE(later_applied.Wait());
  EXPECT_FALSE(later_ack.Wait(std::chrono::milliseconds(30)));
  EXPECT_FALSE(first_ack.IsSet());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("NOT_FOUND", Get("later"));
  release.Set();
  first.join();
  later.join();
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("two", Get("later"));
}

TEST_P(AsyncCommitExecutorTest, EagerSequenceEmptyBatchesWaitForPriorSequence) {
  Options options = PoolOptions();
  ASSERT_OK(DisableLazyAdvanceSequence(&options));
  Reopen(options);
  WriteBatch initial_empty, initial_metadata;
  ASSERT_OK(db_->Write(Writes(), &initial_empty));
  ASSERT_OK(initial_metadata.PutLogData("initial metadata"));
  ASSERT_OK(db_->Write(Writes(), &initial_metadata));
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  ASSERT_EQ(0U, db_->GetLatestSequenceNumber());
  PoolEvent applying, release, empty_wal, log_wal, empty_ack, log_ack;
  std::atomic<int> wal_count{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 1) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void*) {
    const int count = ++wal_count;
    if (count == 2) empty_wal.Set();
    if (count == 3) log_wal.Set();
  });
  sp->EnableProcessing();
  std::thread first([&] { EXPECT_OK(db_->Put(Writes(), "first", "v")); });
  EXPECT_TRUE(applying.Wait());
  std::thread empty([&] {
    WriteBatch batch;
    EXPECT_OK(db_->Write(Writes(), &batch));
    empty_ack.Set();
  });
  EXPECT_TRUE(empty_wal.Wait());
  std::thread log([&] {
    WriteBatch batch;
    EXPECT_OK(batch.PutLogData("metadata"));
    EXPECT_OK(db_->Write(Writes(), &batch));
    log_ack.Set();
  });
  EXPECT_TRUE(log_wal.Wait());
  EXPECT_FALSE(empty_ack.Wait(std::chrono::milliseconds(30)));
  EXPECT_FALSE(log_ack.Wait(std::chrono::milliseconds(30)));
  release.Set();
  first.join();
  empty.join();
  log.join();
  EXPECT_EQ(1U, db_->GetLatestSequenceNumber());
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, EagerSequenceReadsUsePublishedViews) {
  Options options = PoolOptions();
  ASSERT_OK(DisableLazyAdvanceSequence(&options));
  Reopen(options);
  ASSERT_OK(db_->Put(Writes(), "existing", "old"));
  PoolEvent applying, release, acknowledged;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->EnableProcessing();
  std::thread writer([&] {
    EXPECT_OK(db_->Put(Writes(), "pending", "new"));
    acknowledged.Set();
  });
  EXPECT_TRUE(applying.Wait());
  // This thread has already written successfully. Its new reads must not
  // wait for the outstanding sequence allocated to another caller.
  ReadOptions read;
  read.deadline = std::chrono::microseconds(env_->NowMicros() + 20000);
  std::string value;
  EXPECT_OK(db_->Get(read, "existing", &value));
  EXPECT_EQ("old", value);
  EXPECT_TRUE(db_->Get(read, "pending", &value).IsNotFound());
  std::vector<std::string> values;
  auto statuses =
      db_->MultiGet(read, {Slice("existing"), Slice("pending")}, &values);
  EXPECT_OK(statuses[0]);
  EXPECT_TRUE(statuses[1].IsNotFound());
  EXPECT_EQ("old", values[0]);
  const Snapshot* snapshot = db_->GetSnapshot();
  std::unique_ptr<Iterator> iter(db_->NewIterator(read));
  iter->Seek("pending");
  EXPECT_FALSE(iter->Valid());
  EXPECT_OK(iter->status());
  EXPECT_FALSE(acknowledged.IsSet());
  release.Set();
  writer.join();
  EXPECT_EQ("new", Get("pending"));
  ReadOptions historical;
  historical.snapshot = snapshot;
  EXPECT_TRUE(db_->Get(historical, "pending", &value).IsNotFound());
  iter->Seek("pending");
  EXPECT_FALSE(iter->Valid());
  EXPECT_OK(iter->status());
  iter.reset();
  db_->ReleaseSnapshot(snapshot);
}

TEST_P(AsyncCommitExecutorTest, EagerSequenceApplyFailureReachesAllCallers) {
  Options options = PoolOptions();
  ASSERT_OK(DisableLazyAdvanceSequence(&options));
  options.avoid_flush_during_shutdown = true;
  Reopen(options);
  PoolEvent forming, form, follower_queued, applying, release;
  std::atomic<bool> first_group{true};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeGroup", [&](void*) {
    if (first_group.exchange(false)) {
      forming.Set();
      EXPECT_TRUE(form.Wait());
    }
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    if (static_cast<WriteThread::Writer*>(arg)->state.load() !=
        WriteThread::STATE_GROUP_LEADER)
      follower_queued.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void* arg) {
    *static_cast<Status*>(arg) =
        Status::IOError("strong visibility apply failed");
  });
  sp->EnableProcessing();
  Status leader_status, follower_status;
  std::thread leader(
      [&] { leader_status = db_->Put(Writes(), "first", "one"); });
  EXPECT_TRUE(forming.Wait());
  std::thread follower(
      [&] { follower_status = db_->Put(Writes(), "second", "two"); });
  EXPECT_TRUE(follower_queued.Wait());
  form.Set();
  EXPECT_TRUE(applying.Wait());
  release.Set();
  leader.join();
  follower.join();
  EXPECT_TRUE(leader_status.IsIOError());
  EXPECT_TRUE(follower_status.IsIOError());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(dbfull()->WaitForAsyncCommit().IsIOError());
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_TRUE(db_->Close().IsIOError());
  Reopen(options);
  EXPECT_EQ("one", Get("first"));
  EXPECT_EQ("two", Get("second"));
}

TEST_P(AsyncCommitExecutorTest,
       EagerSequenceWALFailureReturnsWithoutPublication) {
  Options options = PoolOptions();
  ASSERT_OK(DisableLazyAdvanceSequence(&options));
  Reopen(options);
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALStatus", [&](void* arg) {
    *static_cast<IOStatus*>(arg) =
        IOStatus::IOError("strong visibility WAL failed");
  });
  sp->EnableProcessing();
  EXPECT_TRUE(db_->Put(Writes(), "failed", "v").IsIOError());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(dbfull()->WaitForAsyncCommit().IsIOError());
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_TRUE(db_->Close().IsIOError());
}

TEST_P(AsyncCommitExecutorTest, EagerSequenceLaterErrorWakesEarlierWaiter) {
  Options options = PoolOptions();
  if (options.async_memtable_write_threads < 2) return;
  ASSERT_OK(DisableLazyAdvanceSequence(&options));
  Reopen(options);
  PoolEvent applying, release, first_ack, later_ack;
  static thread_local SequenceNumber current_sequence = 0;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeRange", [&](void* arg) {
    current_sequence = *static_cast<SequenceNumber*>(arg);
    if (current_sequence == 1) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void* arg) {
    if (current_sequence == 2)
      *static_cast<Status*>(arg) = Status::IOError("later strong group failed");
  });
  sp->EnableProcessing();
  Status first_status, later_status;
  std::thread first([&] {
    first_status = db_->Put(Writes(), "first", "one");
    first_ack.Set();
  });
  EXPECT_TRUE(applying.Wait());
  std::thread later([&] {
    later_status = db_->Put(Writes(), "later", "two");
    later_ack.Set();
  });
  // A terminal error must wake every publication waiter, even when another
  // worker is still holding the uncompleted prefix. Cleanup drains separately.
  EXPECT_TRUE(later_ack.Wait(std::chrono::seconds(2)));
  EXPECT_TRUE(first_ack.Wait(std::chrono::seconds(2)));
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  release.Set();
  first.join();
  later.join();
  EXPECT_TRUE(first_status.IsIOError());
  EXPECT_TRUE(later_status.IsIOError());
  EXPECT_TRUE(dbfull()->WaitForAsyncCommit().IsIOError());
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_TRUE(db_->Close().IsIOError());
}

TEST_P(AsyncCommitExecutorTest, LeaderReturnsAndNextWALRunsBeforeApply) {
  Reopen(PoolOptions());
  PoolEvent applying, release, first_ack, second_ack, read_waited;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 1) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  sp->EnableProcessing();
  std::thread first([&] {
    WriteBatch batch(0, 0, 8);
    EXPECT_OK(batch.Put("first", "owned"));
    EXPECT_OK(db_->Write(Writes(), &batch));
    batch.Clear();
    EXPECT_OK(batch.Put("clobbered", "not submitted"));
    first_ack.Set();
    ReadOptions deadline;
    deadline.deadline = std::chrono::microseconds(env_->NowMicros() + 20000);
    std::string value;
    EXPECT_TRUE(db_->Get(deadline, "first", &value).IsTimedOut());
    read_waited.Set();
    EXPECT_TRUE(release.Wait());
    EXPECT_OK(db_->Get(ReadOptions(), "first", &value));
    EXPECT_EQ("owned", value);
  });
  EXPECT_TRUE(applying.Wait());
  const bool early_first = first_ack.Wait(std::chrono::seconds(2));
  std::thread second([&] {
    EXPECT_OK(db_->Put(Writes(), "second", "next WAL"));
    second_ack.Set();
  });
  const bool early_second = second_ack.Wait(std::chrono::seconds(2));
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  if (early_first) {
    EXPECT_TRUE(read_waited.Wait());
  }
  release.Set();
  first.join();
  second.join();
  EXPECT_TRUE(early_first);
  EXPECT_TRUE(early_second);
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("owned", Get("first"));
  EXPECT_EQ("next WAL", Get("second"));
  EXPECT_EQ("NOT_FOUND", Get("clobbered"));
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, SubmissionDoesNotWaitForCompletionMutex) {
  Options options = PoolOptions();
  Reopen(options);
  PoolEvent applying, release_apply, second_wal, release_wal;
  PoolEvent completing, release_completion, acknowledged, next_applied;
  std::atomic<int> wal_count{0};
  std::atomic<bool> first_completion{true};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 1) {
      applying.Set();
      EXPECT_TRUE(release_apply.Wait());
    } else {
      next_applied.Set();
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void*) {
    if (++wal_count == 2) {
      second_wal.Set();
      EXPECT_TRUE(release_wal.Wait());
    }
  });
  sp->SetCallBack("AsyncCommitExecutor::Complete:Locked", [&](void*) {
    if (first_completion.exchange(false)) {
      completing.Set();
      EXPECT_TRUE(release_completion.Wait());
    }
  });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "first", "v"));
  EXPECT_TRUE(applying.Wait());
  std::thread second([&] {
    EXPECT_OK(db_->Put(Writes(), "second", "v"));
    acknowledged.Set();
  });
  EXPECT_TRUE(second_wal.Wait());
  release_apply.Set();
  EXPECT_TRUE(completing.Wait());
  release_wal.Set();
  const bool independent = acknowledged.Wait(std::chrono::seconds(1));
  const bool apply_independent = options.async_memtable_write_threads == 1 ||
                                 next_applied.Wait(std::chrono::seconds(1));
  release_completion.Set();
  second.join();
  EXPECT_TRUE(independent);
  EXPECT_TRUE(apply_independent);
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, PollingFindsWorkWithoutWake) {
  Options options = PoolOptions();
  options.async_memtable_write_threads = 1;
  options.allow_concurrent_memtable_write = false;
  Reopen(options);
  PoolEvent polling, release_poll, enqueued, release_wake, applying;
  std::atomic<bool> first_poll{true};
  std::atomic<int> submitted{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("AsyncCommitExecutor::Run:IdlePoll", [&](void*) {
    if (first_poll.exchange(false)) {
      polling.Set();
      EXPECT_TRUE(release_poll.Wait());
    }
  });
  sp->SetCallBack("AsyncCommitExecutor::Submit:BeforeWake", [&](void*) {
    if (++submitted == 2) {
      enqueued.Set();
      EXPECT_TRUE(release_wake.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) != 1)
      applying.Set();
  });
  sp->EnableProcessing();
  // A wake makes an already sleeping worker enter its empty polling period.
  EXPECT_OK(db_->Put(Writes(), "kick", "v"));
  const bool saw_poll = polling.Wait(std::chrono::seconds(1));
  std::thread writer([&] { EXPECT_OK(db_->Put(Writes(), "poll", "v")); });
  const bool published = enqueued.Wait(std::chrono::seconds(1));
  release_poll.Set();
  const bool applied_without_wake = applying.Wait(std::chrono::seconds(1));
  release_wake.Set();
  writer.join();
  EXPECT_TRUE(saw_poll);
  EXPECT_TRUE(published);
  EXPECT_TRUE(applied_without_wake);
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, LastEnqueueCannotLoseWake) {
  Options options = PoolOptions();
  options.async_memtable_write_threads = 1;
  options.allow_concurrent_memtable_write = false;
  Reopen(options);
  PoolEvent sleeping, release_sleep;
  std::atomic<bool> first_sleep{true};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("AsyncCommitExecutor::Run:BeforeSleep", [&](void*) {
    if (first_sleep.exchange(false)) {
      sleeping.Set();
      EXPECT_TRUE(release_sleep.Wait());
    }
  });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "kick", "v"));
  const bool saw_sleep = sleeping.Wait(std::chrono::seconds(1));
  EXPECT_OK(db_->Put(Writes(), "last", "no subsequent request"));
  release_sleep.Set();
  EXPECT_TRUE(saw_sleep);
  // A missing wake would leave this read waiting without a later enqueue.
  ReadOptions deadline;
  deadline.deadline = std::chrono::microseconds(env_->NowMicros() + 1000000);
  std::string value;
  const Status observed = db_->Get(deadline, "last", &value);
  EXPECT_OK(observed);
  EXPECT_EQ("no subsequent request", value);
  // Preserve the failed observation, then wake a stranded worker so the
  // regression reports a failure instead of hanging in the cleanup drain.
  if (!observed.ok()) {
    EXPECT_OK(db_->Put(Writes(), "cleanup", "wake"));
  }
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, WALHandoffRespectsSyncNotificationOrder) {
  Reopen(PoolOptions());
  PoolEvent first_wal, release_wal, second_queued, notifying, release_notify;
  PoolEvent second_wal;
  std::atomic<bool> first_notification{true};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void* arg) {
    const auto sequence = WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg));
    if (sequence == 1) {
      first_wal.Set();
      EXPECT_TRUE(release_wal.Wait());
    } else {
      second_wal.Set();
    }
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    auto* w = static_cast<WriteThread::Writer*>(arg);
    if (w->state.load() != WriteThread::STATE_GROUP_LEADER) second_queued.Set();
  });
  sp->SetCallBack("WriteThread::NotifyAsyncFollowers:Before", [&](void*) {
    if (first_notification.exchange(false)) {
      notifying.Set();
      EXPECT_TRUE(release_notify.Wait());
    }
  });
  sp->EnableProcessing();
  std::thread first([&] { EXPECT_OK(db_->Put(Writes(), "first", "v")); });
  EXPECT_TRUE(first_wal.Wait());
  std::thread second([&] { EXPECT_OK(db_->Put(Writes(), "second", "v")); });
  EXPECT_TRUE(second_queued.Wait());
  release_wal.Set();
  EXPECT_TRUE(notifying.Wait());
  // Synchronous groups notify their callers while retaining the WAL gate so
  // returning followers can join the next group. Non-sync groups hand off the
  // next leader first. Hold notifications to distinguish these two protocols.
  EXPECT_EQ(!Writes().sync, second_wal.Wait(std::chrono::seconds(1)));
  release_notify.Set();
  first.join();
  second.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, OptionsAndConcurrentFactoryValidation) {
  Options options = PoolOptions();
  Options invalid = options;
  invalid.async_memtable_write_threads = -1;
  EXPECT_TRUE(TryReopen(invalid).IsInvalidArgument());
  invalid = options;
  invalid.async_memtable_write = false;
  EXPECT_TRUE(TryReopen(invalid).IsInvalidArgument());
  invalid = options;
  invalid.max_async_memtable_write_pending_groups = 0;
  EXPECT_TRUE(TryReopen(invalid).IsInvalidArgument());
  invalid = options;
  invalid.async_memtable_write_threads = 2;
  invalid.allow_concurrent_memtable_write = false;
  EXPECT_TRUE(TryReopen(invalid).IsInvalidArgument());
  Reopen(options);
  if (options.async_memtable_write_threads > 1) {
    ColumnFamilyOptions cf_options;
    cf_options.memtable_factory.reset(new VectorRepFactory());
    ColumnFamilyHandle* cf = nullptr;
    EXPECT_FALSE(db_->CreateColumnFamily(cf_options, "unsupported", &cf).ok());
    EXPECT_EQ(nullptr, cf);
  }
}

TEST_P(AsyncCommitExecutorTest, ConcurrentProtectedCrossCFWritesFlushRecovery) {
  Options options = PoolOptions();
  options.write_buffer_size = 64 * 1024;
  options.max_write_buffer_number = 3;
  options.memtable_prefix_bloom_size_ratio = 0.1;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  Reopen(options);
  CreateAndReopenWithCF({"second"}, options);
  std::vector<std::thread> callers;
  for (int t = 0; t < 4; ++t) {
    callers.emplace_back([&, t] {
      for (int n = 0; n < 80; ++n) {
        const std::string key = std::to_string(t) + ":" + std::to_string(n);
        WriteBatch batch(0, 0, 8);
        EXPECT_OK(batch.Put(handles_[0], key, std::string(512, 'a')));
        EXPECT_OK(batch.Put(handles_[1], key, std::string(512, 'b')));
        EXPECT_OK(batch.DeleteRange(handles_[0], "z", "z"));
        EXPECT_OK(db_->Write(Writes(), &batch));
        batch.Clear();
        std::string value;
        EXPECT_OK(db_->Get(ReadOptions(), handles_[1], key, &value));
        EXPECT_EQ(std::string(512, 'b'), value);
      }
    });
  }
  for (auto& caller : callers) caller.join();
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
  EXPECT_EQ(960U, db_->GetLatestSequenceNumber());
  ASSERT_OK(db_->Flush(FlushOptions(), handles_));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "second"}, options);
  for (int t = 0; t < 4; ++t) {
    EXPECT_EQ(std::string(512, 'a'), Get(0, std::to_string(t) + ":79"));
    EXPECT_EQ(std::string(512, 'b'), Get(1, std::to_string(t) + ":79"));
  }
  EXPECT_EQ(960U, db_->GetLatestSequenceNumber());
}

TEST_P(AsyncCommitExecutorTest, PostProcessCountsAndFirstSequence) {
  Options options = PoolOptions();
  options.write_buffer_size = 64 * 1024 * 1024;
  Reopen(options);
  for (int n = 0; n < 128; ++n) {
    WriteBatch batch(0, 0, 8);
    EXPECT_OK(batch.Put("key", std::to_string(n)));
    EXPECT_OK(batch.Delete("deleted"));
    EXPECT_OK(db_->Write(Writes(), &batch));
  }
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  auto* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  EXPECT_EQ(256U, cfd->mem()->num_entries());
  EXPECT_EQ(128U, cfd->mem()->num_deletes());
  EXPECT_EQ(1U, cfd->mem()->GetFirstSequenceNumber());
  EXPECT_LE(cfd->mem()->GetEarliestSequenceNumber(), 1U);
  EXPECT_EQ("127", Get("key"));
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, DeleteRangeSnapshotAndIterator) {
  Options options = PoolOptions();
  Reopen(options);
  ASSERT_OK(db_->Put(Writes(), "b", "before"));
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  const Snapshot* snapshot = db_->GetSnapshot();
  WriteBatch erase(0, 0, 8);
  ASSERT_OK(erase.DeleteRange("a", "d"));
  ASSERT_OK(erase.Put("c", "after"));
  ASSERT_OK(erase.DeleteRange("c", "e"));
  ASSERT_OK(erase.Put("b", "new"));
  ASSERT_OK(db_->Write(Writes(), &erase));
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ("new", Get("b"));
  EXPECT_EQ("NOT_FOUND", Get("c"));
  EXPECT_EQ("before", Get("b", snapshot));
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("b", iter->key().ToString());
  EXPECT_EQ("new", iter->value().ToString());
  iter->Next();
  EXPECT_FALSE(iter->Valid());
  EXPECT_OK(iter->status());
  iter.reset();
  db_->ReleaseSnapshot(snapshot);
  EXPECT_OK(Flush());
  Reopen(options);
  EXPECT_EQ("new", Get("b"));
  EXPECT_EQ("NOT_FOUND", Get("c"));
}

TEST_P(AsyncCommitExecutorTest, GroupQuotaIncludesLogOnlyAndOversizedGroup) {
  Options options = PoolOptions();
  options.max_async_memtable_write_pending_groups = 1;
  options.max_async_memtable_write_pending_bytes = 1;
  Reopen(options);
  PoolEvent applying, release;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->EnableProcessing();
  WriteBatch metadata;
  metadata.PutLogData("no sequence, still consumes group quota");
  EXPECT_OK(db_->Write(Writes(), &metadata));
  EXPECT_TRUE(applying.Wait());
  WriteOptions no_slowdown = Writes();
  no_slowdown.no_slowdown = true;
  EXPECT_TRUE(db_->Write(no_slowdown, &metadata).IsIncomplete());
  EXPECT_TRUE(db_->Put(no_slowdown, "rejected", "v").IsIncomplete());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  PoolEvent capacity_wait;
  sp->SetCallBack("DBImpl::AsyncWriteImpl:CapacityWait",
                  [&](void*) { capacity_wait.Set(); });
  std::thread pending([&] {
    EXPECT_OK(db_->Put(Writes(), "blocking", "admitted after drain"));
  });
  EXPECT_TRUE(capacity_wait.Wait());
  release.Set();
  pending.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_OK(db_->Put(Writes(), "large", std::string(8192, 'v')));
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("admitted after drain", Get("blocking"));
  EXPECT_EQ("NOT_FOUND", Get("rejected"));
}

TEST_P(AsyncCommitExecutorTest, ManagementOperationsWaitForWorkers) {
  Options options = PoolOptions();
  options.write_buffer_size = 128 * 1024;
  Reopen(options);
  const std::string external = dbname_ + "-external.sst";
  SstFileWriter file(EnvOptions(), options);
  ASSERT_OK(file.Open(external));
  ASSERT_OK(file.Put("external", "ingested"));
  ASSERT_OK(file.Finish());
  for (int operation = 0; operation < 6; ++operation) {
    PoolEvent applying, release, blocked, finished;
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    });
    sp->SetCallBack("AsyncCommitExecutor::DrainThrough:Wait",
                    [&](void*) { blocked.Set(); });
    sp->EnableProcessing();
    EXPECT_OK(db_->Put(Writes(), "pending", "value"));
    EXPECT_TRUE(applying.Wait());
    std::thread admin([&] {
      if (operation == 0) {
        EXPECT_OK(Flush());
      }
      if (operation == 1) {
        EXPECT_OK(db_->SetOptions({{"write_buffer_size", "262144"}}));
      }
      if (operation == 2) {
        EXPECT_OK(dbfull()->TEST_SwitchWAL());
      }
      if (operation == 3) {
        ColumnFamilyHandle* cf = nullptr;
        EXPECT_OK(
            db_->CreateColumnFamily(ColumnFamilyOptions(), "created", &cf));
        if (cf) {
          EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
        }
      }
      if (operation == 4) {
        EXPECT_OK(db_->DropColumnFamily(handles_.back()));
      }
      if (operation == 5) {
        EXPECT_OK(
            db_->IngestExternalFile({external}, IngestExternalFileOptions()));
      }
      finished.Set();
    });
    EXPECT_TRUE(blocked.Wait());
    EXPECT_FALSE(finished.IsSet());
    release.Set();
    admin.join();
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
    EXPECT_EQ("value", Get("pending"));
    if (operation == 3) {
      // Obtain a handle for the subsequent Drop barrier case.
      ReopenWithColumnFamilies({kDefaultColumnFamilyName, "created"}, options);
    }
  }
  EXPECT_EQ("ingested", Get("external"));
  EXPECT_OK(env_->DeleteFile(external));
}

TEST_P(AsyncCommitExecutorTest, WALFailureAndTerminalClose) {
  Reopen(PoolOptions());
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALStatus", [&](void* arg) {
    *static_cast<IOStatus*>(arg) = IOStatus::IOError("pool WAL failure");
  });
  sp->EnableProcessing();
  EXPECT_TRUE(db_->Put(Writes(), "failed", "v").IsIOError());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_TRUE(dbfull()->WaitForAsyncCommit().IsIOError());
  ExpectDrained();
  EXPECT_TRUE(db_->Close().IsIOError());
}

TEST_P(AsyncCommitExecutorTest, WALSuccessStillQueuesAfterEarlierApplyError) {
  Options options = PoolOptions();
  options.avoid_flush_during_shutdown = true;
  Reopen(options);
  PoolEvent applying, release_apply, second_wal, release_wal, first_completed;
  std::atomic<int> wal_count{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 1) {
      applying.Set();
      EXPECT_TRUE(release_apply.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void* arg) {
    *static_cast<Status*>(arg) = Status::IOError("previous apply failed");
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void*) {
    if (++wal_count == 2) {
      second_wal.Set();
      EXPECT_TRUE(release_wal.Wait());
    }
  });
  sp->SetCallBack("AsyncCommitExecutor::Complete:Locked",
                  [&](void*) { first_completed.Set(); });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "first", "recovered"));
  EXPECT_TRUE(applying.Wait());
  Status second_status;
  std::thread writer([&] {
    second_status = db_->Put(Writes(), "second", "also recovered");
  });
  EXPECT_TRUE(second_wal.Wait());
  release_apply.Set();
  EXPECT_TRUE(first_completed.Wait());
  release_wal.Set();
  writer.join();
  EXPECT_OK(second_status);
  EXPECT_TRUE(dbfull()->WaitForAsyncCommit().IsIOError());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_TRUE(db_->Close().IsIOError());
  Reopen(options);
  EXPECT_EQ("recovered", Get("first"));
  EXPECT_EQ("also recovered", Get("second"));
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
}

TEST_P(AsyncCommitExecutorTest, PartialInsertionFailureAccountsAndRecovers) {
  Options options = PoolOptions();
  options.avoid_flush_during_shutdown = true;
  Reopen(options);
  auto* sp = SyncPoint::GetInstance();
  std::atomic<int> encoded{0};
  sp->SetCallBack("MemTable::Add:Encoded", [&](void* arg) {
    if (++encoded == 2) {
      auto* bytes = static_cast<Slice*>(arg);
      const_cast<char*>(bytes->data())[bytes->size() - 1] ^= 1;
    }
  });
  sp->EnableProcessing();
  WriteBatch batch(0, 0, 8);
  ASSERT_OK(batch.Put("first", "one"));
  ASSERT_OK(batch.Put("second", "two"));
  Status result = db_->Write(Writes(), &batch);
  // The WAL acknowledgment decision is frozen at submission, even if the
  // worker reports corruption before this caller gets CPU time to return.
  EXPECT_OK(result);
  EXPECT_TRUE(dbfull()->WaitForAsyncCommit().IsCorruption());
  auto* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  EXPECT_EQ(1U, cfd->mem()->num_entries());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  ExpectDrained();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_TRUE(db_->Close().IsCorruption());
  Reopen(options);
  EXPECT_EQ("one", Get("first"));
  EXPECT_EQ("two", Get("second"));
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
}

TEST_P(AsyncCommitExecutorTest,
       ErrorFreezesPublicationAndDrainWaitsForCleanup) {
  Options options = PoolOptions();
  if (options.async_memtable_write_threads < 2) return;
  Reopen(options);
  PoolEvent first_started, release_first, later_done, failed, drain_blocked,
      drained;
  auto* sp = SyncPoint::GetInstance();
  static thread_local SequenceNumber applying_sequence = 0;
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    applying_sequence =
        WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg));
    if (applying_sequence == 1) {
      first_started.Set();
      EXPECT_TRUE(release_first.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void* arg) {
    if (applying_sequence == 2) later_done.Set();
    if (applying_sequence == 3) {
      *static_cast<Status*>(arg) = Status::IOError("later pool group failed");
      failed.Set();
    }
  });
  sp->SetCallBack("AsyncCommitExecutor::DrainThrough:Wait",
                  [&](void*) { drain_blocked.Set(); });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "first", "v1"));
  EXPECT_TRUE(first_started.Wait());
  EXPECT_OK(db_->Put(Writes(), "later", "v2"));
  EXPECT_TRUE(later_done.Wait());
  EXPECT_OK(db_->Put(Writes(), "failed", "v3"));
  EXPECT_TRUE(failed.Wait());
  std::thread barrier([&] {
    EXPECT_TRUE(dbfull()->WaitForAsyncCommit().IsIOError());
    drained.Set();
  });
  EXPECT_TRUE(drain_blocked.Wait());
  EXPECT_FALSE(drained.IsSet());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions(), "failed", &value).IsIOError());
  release_first.Set();
  barrier.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  ExpectDrained();
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(db_->Close().IsIOError());
  Reopen(options);
  EXPECT_EQ("v1", Get("first"));
  EXPECT_EQ("v2", Get("later"));
  EXPECT_EQ("v3", Get("failed"));
}

TEST_P(AsyncCommitExecutorTest, MergeIsExclusiveBetweenOrdinaryGroups) {
  Options options = PoolOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen(options);
  for (int n = 0; n < 12; ++n) {
    EXPECT_OK(db_->Merge(Writes(), "merged", std::to_string(n)));
    EXPECT_OK(db_->Put(Writes(), "ordinary", std::to_string(n)));
  }
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ("0,1,2,3,4,5,6,7,8,9,10,11", Get("merged"));
  EXPECT_EQ("11", Get("ordinary"));
  EXPECT_EQ(24U, db_->GetLatestSequenceNumber());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, OneGroupKeepsRangesOnOneWorker) {
  Options options = PoolOptions();
  if (options.async_memtable_write_threads < 2) return;
  Reopen(options);
  PoolEvent forming, form, follower_queued, first_range, release, second_range;
  PoolEvent follower_ack, leader_ack;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeGroup", [&](void*) {
    forming.Set();
    EXPECT_TRUE(form.Wait());
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    auto* w = static_cast<WriteThread::Writer*>(arg);
    if (w->state.load() != WriteThread::STATE_GROUP_LEADER)
      follower_queued.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeRange", [&](void* arg) {
    if (*static_cast<SequenceNumber*>(arg) == 1) {
      first_range.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:RangeComplete", [&](void* arg) {
    if (*static_cast<SequenceNumber*>(arg) == 2) second_range.Set();
  });
  sp->EnableProcessing();
  std::thread leader([&] {
    EXPECT_OK(db_->Put(Writes(), "first", "one"));
    leader_ack.Set();
  });
  EXPECT_TRUE(forming.Wait());
  std::thread follower([&] {
    EXPECT_OK(db_->Put(Writes(), "second", "two"));
    follower_ack.Set();
  });
  EXPECT_TRUE(follower_queued.Wait());
  form.Set();
  EXPECT_TRUE(first_range.Wait());
  EXPECT_FALSE(second_range.Wait(std::chrono::milliseconds(100)));
  EXPECT_TRUE(follower_ack.Wait());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(leader_ack.Wait());
  release.Set();
  leader.join();
  follower.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_TRUE(second_range.IsSet());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("one", Get("first"));
  EXPECT_EQ("two", Get("second"));
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, WriteBufferManagerStallMakesProgress) {
  Options options = PoolOptions();
  options.write_buffer_size = 64 * 1024;
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(128 * 1024, nullptr, true);
  options.max_write_buffer_number = 4;
  Reopen(options);
  for (int n = 0; n < 256; ++n)
    ASSERT_OK(db_->Put(Writes(), std::to_string(n), std::string(8192, 'v')));
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ(256U, db_->GetLatestSequenceNumber());
  ExpectDrained();
  EXPECT_EQ(std::string(8192, 'v'), Get("255"));
}

TEST_P(AsyncCommitExecutorTest, RandomHotKeysMatchWALSerialModel) {
  Options options = PoolOptions();
  Reopen(options);
  std::vector<std::string> wal;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void* arg) {
    auto* batch = static_cast<WriteBatch*>(arg);
    wal.push_back(WriteBatchInternal::Contents(batch).ToString());
  });
  sp->EnableProcessing();
  std::vector<std::thread> callers;
  for (int t = 0; t < 4; ++t) {
    callers.emplace_back([&, t] {
      for (int n = 0; n < 40; ++n) {
        WriteBatch batch(0, 0, 8);
        std::string key = "k" + std::to_string((n * 7 + t * 3) % 10);
        EXPECT_OK(batch.Put(key, std::to_string(t) + ":" + std::to_string(n)));
        if (n % 3 == 0) {
          EXPECT_OK(batch.Delete("k8"));
        }
        if (n % 7 == 0) {
          EXPECT_OK(batch.DeleteRange("k2", "k5"));
        }
        EXPECT_OK(db_->Write(Writes(), &batch));
      }
    });
  }
  for (auto& caller : callers) caller.join();
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  class Model : public WriteBatch::Handler {
   public:
    std::map<std::string, std::string> values;
    Status PutCF(uint32_t, const Slice& key, const Slice& value) override {
      values[key.ToString()] = value.ToString();
      return Status::OK();
    }
    Status DeleteCF(uint32_t, const Slice& key) override {
      values.erase(key.ToString());
      return Status::OK();
    }
    Status DeleteRangeCF(uint32_t, const Slice& begin,
                         const Slice& end) override {
      values.erase(values.lower_bound(begin.ToString()),
                   values.lower_bound(end.ToString()));
      return Status::OK();
    }
  } model;
  SequenceNumber sequence = 0;
  for (const auto& bytes : wal) {
    WriteBatch batch;
    ASSERT_OK(WriteBatchInternal::SetContents(&batch, bytes));
    EXPECT_EQ(sequence + 1, WriteBatchInternal::Sequence(&batch));
    sequence += WriteBatchInternal::Count(&batch);
    ASSERT_OK(batch.Iterate(&model));
  }
  EXPECT_EQ(sequence, db_->GetLatestSequenceNumber());
  for (int i = 0; i < 10; ++i) {
    const std::string key = "k" + std::to_string(i);
    auto found = model.values.find(key);
    EXPECT_EQ(found == model.values.end() ? "NOT_FOUND" : found->second,
              Get(key));
  }
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, MergeFenceBlocksBothEarlierAndLaterGroups) {
  Options options = PoolOptions();
  options.async_memtable_write_groups_per_fetch = 64;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen(options);
  PoolEvent first, release_first, merged, release_merge, later, fence1, fence2;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    SequenceNumber seq =
        WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg));
    if (seq == 1) {
      first.Set();
      EXPECT_TRUE(release_first.Wait());
    }
    if (seq == 2) {
      merged.Set();
      EXPECT_TRUE(release_merge.Wait());
    }
    if (seq >= 3) later.Set();
  });
  sp->SetCallBack("AsyncCommitExecutor::Run:ExclusiveWait", [&](void* arg) {
    const uint64_t target = *static_cast<uint64_t*>(arg);
    if (target == 1) fence1.Set();
    if (target == 2) fence2.Set();
  });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "first", "one"));
  EXPECT_TRUE(first.Wait());
  EXPECT_OK(db_->Merge(Writes(), "merged", "M"));
  EXPECT_OK(db_->Put(Writes(), "later", "three"));
  if (options.async_memtable_write_threads > 1) {
    EXPECT_TRUE(fence1.Wait());
  }
  EXPECT_FALSE(merged.IsSet());
  EXPECT_FALSE(later.IsSet());
  release_first.Set();
  EXPECT_TRUE(merged.Wait());
  EXPECT_OK(db_->Put(Writes(), "last", "four"));
  if (options.async_memtable_write_threads > 1) {
    EXPECT_TRUE(fence2.Wait());
  }
  EXPECT_FALSE(later.IsSet());
  release_merge.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ("M", Get("merged"));
  EXPECT_EQ(4U, db_->GetLatestSequenceNumber());
}

TEST_P(AsyncCommitExecutorTest, ReleasedCFHandleDoesNotOwnWorkerLifetime) {
  Options options = PoolOptions();
  Reopen(options);
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(
      db_->CreateColumnFamily(ColumnFamilyOptions(options), "second", &cf));
  PoolEvent applying, release;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->EnableProcessing();
  WriteBatch batch(0, 0, 8);
  EXPECT_OK(batch.Put(cf, "key", "owned"));
  EXPECT_OK(db_->Write(Writes(), &batch));
  EXPECT_TRUE(applying.Wait());
  batch.Clear();
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
  release.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "second"}, options);
  EXPECT_EQ("owned", Get(1, "key"));
}

#if defined(__linux__)
int PoolCrashWriter(const std::string& path, int mode) {
  Options options;
  options.create_if_missing = true;
  options.async_memtable_write = true;
  options.enable_pipelined_write = (mode & 1) != 0;
  options.async_memtable_write_threads = 1 << (mode / 4);
  options.allow_concurrent_memtable_write =
      options.async_memtable_write_threads > 1;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB* db = nullptr;
  if (!DB::Open(options, path, &db).ok()) return 11;
  ColumnFamilyHandle* cf = nullptr;
  if (!db->CreateColumnFamily(ColumnFamilyOptions(options), "second", &cf).ok())
    return 12;
  PoolEvent never;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply",
                  [&](void*) { never.Wait(); });
  sp->EnableProcessing();
  WriteOptions wo;
  wo.sync = (mode & 2) != 0;
  WriteBatch batch(0, 0, 8);
  if (!batch.Put("key", "old").ok() ||
      !batch.Put(cf, "cf-key", "cf-value").ok() ||
      !batch.DeleteRange("z", "z").ok() || !batch.Merge("merged", "M").ok())
    return 13;
  if (!db->Write(wo, &batch).ok()) return 14;
  batch.Clear();
  if (!db->Put(wo, "key", "new").ok()) return 15;
  batch.PutLogData("log-only acknowledged too");
  if (!db->Write(wo, &batch).ok()) return 16;
  // All caller APIs returned, while every executing worker is paused before
  // insertion. Bypass DB destruction so recovery must rely entirely on WAL.
  std::_Exit(0);
}

TEST_P(AsyncCommitExecutorTest, AllCallersAcknowledgedWALSurvivesProcessExit) {
  Options options = PoolOptions();
  const std::string path = dbname_ + "-pool-crash";
  ASSERT_OK(DestroyDB(path, options));
  std::string mode = std::to_string(GetParam());
  pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    execl("/proc/self/exe", "async_commit_executor_test", "--pool-crash-writer",
          path.c_str(), mode.c_str(), static_cast<char*>(nullptr));
    std::_Exit(127);
  }
  int status = 0;
  ASSERT_EQ(child, waitpid(child, &status, 0));
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(0, WEXITSTATUS(status));
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  for (int reopen = 0; reopen < 3; ++reopen) {
    options.async_memtable_write = reopen != 0;
    options.async_memtable_write_threads = reopen == 2 ? 2 : 0;
    options.allow_concurrent_memtable_write = reopen == 2;
    DB* recovered = nullptr;
    std::vector<ColumnFamilyHandle*> handles;
    ASSERT_OK(
        DB::Open(DBOptions(options), path,
                 {{kDefaultColumnFamilyName, ColumnFamilyOptions(options)},
                  {"second", ColumnFamilyOptions(options)}},
                 &handles, &recovered));
    std::string value;
    EXPECT_OK(recovered->Get(ReadOptions(), handles[0], "key", &value));
    EXPECT_EQ("new", value);
    EXPECT_OK(recovered->Get(ReadOptions(), handles[1], "cf-key", &value));
    EXPECT_EQ("cf-value", value);
    EXPECT_OK(recovered->Get(ReadOptions(), handles[0], "merged", &value));
    EXPECT_EQ("M", value);
    EXPECT_EQ(5U, recovered->GetLatestSequenceNumber());
    for (auto* handle : handles)
      EXPECT_OK(recovered->DestroyColumnFamilyHandle(handle));
    delete recovered;
  }
  ASSERT_OK(DestroyDB(path, options));
}
#endif

TEST_P(AsyncCommitExecutorTest, RejectedGroupCleansTargetsBeforeWALHandoff) {
  Options options = PoolOptions();
  options.max_async_memtable_write_pending_groups = 1;
  Reopen(options);
  PoolEvent applying, release_apply, rejected_cleanup, release_cleanup,
      gate_wait;
  PoolEvent flushed;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release_apply.Wait());
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:RejectedGroupCleanup", [&](void*) {
    rejected_cleanup.Set();
    EXPECT_TRUE(release_cleanup.Wait());
  });
  sp->SetCallBack("WriteThread::EnterUnbatched:Wait",
                  [&](void*) { gate_wait.Set(); });
  sp->EnableProcessing();
  EXPECT_OK(db_->Put(Writes(), "accepted", "one"));
  EXPECT_TRUE(applying.Wait());
  std::thread rejected([&] {
    WriteOptions no_slowdown = Writes();
    no_slowdown.no_slowdown = true;
    EXPECT_TRUE(db_->Put(no_slowdown, "rejected", "no").IsIncomplete());
  });
  EXPECT_TRUE(rejected_cleanup.Wait());
  release_apply.Set();
  std::thread admin([&] {
    EXPECT_OK(Flush());
    flushed.Set();
  });
  EXPECT_TRUE(gate_wait.Wait());
  EXPECT_FALSE(flushed.IsSet());
  release_cleanup.Set();
  rejected.join();
  admin.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ("one", Get("accepted"));
  EXPECT_EQ("NOT_FOUND", Get("rejected"));
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, OwnedProtectedMissingCFAndEmptyRangeRecovery) {
  Options options = PoolOptions();
  options.avoid_flush_during_shutdown = true;
  Reopen(options);
  WriteBatch batch(0, 0, 8);
  ASSERT_OK(batch.Put("kept", "value"));
  ASSERT_OK(WriteBatchInternal::Put(&batch, 99999, "missing", "ignored"));
  batch.PutLogData("metadata between records");
  ASSERT_OK(batch.DeleteRange("z", "z"));
  WriteOptions wo = Writes();
  wo.ignore_missing_column_families = true;
  ASSERT_OK(db_->Write(wo, &batch));
  batch.Clear();
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  EXPECT_EQ(3U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("value", Get("kept"));
  ASSERT_OK(WriteBatchInternal::Put(&batch, 99999, "missing", "reject"));
  EXPECT_TRUE(db_->Write(Writes(), &batch).IsInvalidArgument());
  EXPECT_EQ(3U, db_->GetLatestSequenceNumber());
  ASSERT_OK(db_->Put(Writes(), "next", "four"));
  ASSERT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
  options.async_memtable_write = false;
  options.async_memtable_write_threads = 0;
  Reopen(options);
  EXPECT_EQ(4U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("value", Get("kept"));
  EXPECT_EQ("four", Get("next"));
}

TEST_P(AsyncCommitExecutorTest, CloseDrainsAfterAllCallersHaveReturned) {
  Options options = PoolOptions();
  options.avoid_flush_during_shutdown = true;
  Reopen(options);
  PoolEvent applying, release, drain_wait, closed;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("AsyncCommitExecutor::DrainThrough:Wait",
                  [&](void*) { drain_wait.Set(); });
  sp->EnableProcessing();
  {
    WriteBatch batch(0, 0, 8);
    ASSERT_OK(batch.Put("owned-at-close", std::string(1024, 'v')));
    ASSERT_OK(db_->Write(Writes(), &batch));
  }
  EXPECT_TRUE(applying.Wait());
  // No caller API is active here. Only the DB-owned worker retains the group.
  std::thread closer([&] {
    EXPECT_OK(db_->Close());
    closed.Set();
  });
  EXPECT_TRUE(drain_wait.Wait());
  EXPECT_FALSE(closed.IsSet());
  release.Set();
  closer.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  Reopen(options);
  EXPECT_EQ(std::string(1024, 'v'), Get("owned-at-close"));
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, AtomicFlushWaitsForAcknowledgedCrossCFGroup) {
  Options options = PoolOptions();
  // This version rejects atomic_flush with the native pipelined option on.
  if (options.enable_pipelined_write) return;
  options.atomic_flush = true;
  Reopen(options);
  CreateAndReopenWithCF({"second"}, options);
  PoolEvent applying, release, drain_wait, flushed;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("AsyncCommitExecutor::DrainThrough:Wait",
                  [&](void*) { drain_wait.Set(); });
  sp->EnableProcessing();
  WriteBatch batch(0, 0, 8);
  ASSERT_OK(batch.Put(handles_[0], "key", "first"));
  ASSERT_OK(batch.Put(handles_[1], "key", "second"));
  ASSERT_OK(db_->Write(Writes(), &batch));
  EXPECT_TRUE(applying.Wait());
  std::thread admin([&] {
    EXPECT_OK(db_->Flush(FlushOptions(), handles_));
    flushed.Set();
  });
  EXPECT_TRUE(drain_wait.Wait());
  EXPECT_FALSE(flushed.IsSet());
  release.Set();
  admin.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "second"}, options);
  EXPECT_EQ("first", Get(0, "key"));
  EXPECT_EQ("second", Get(1, "key"));
  ExpectDrained();
}

INSTANTIATE_TEST_CASE_P(PoolModes, AsyncCommitExecutorTest,
                        testing::Range(0, 16));
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
#if defined(__linux__)
  if (argc == 4 && std::string(argv[1]) == "--pool-crash-writer")
    return ROCKSDB_NAMESPACE::PoolCrashWriter(argv[2], std::stoi(argv[3]));
#endif
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
