// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include <atomic>
#include <condition_variable>
#include <map>
#if defined(__linux__)
#include <sys/wait.h>
#include <unistd.h>
#endif
#include <mutex>
#include <thread>

#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "db/write_thread.h"
#include "port/stack_trace.h"
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
    options.enable_async_commit = true;
    options.enable_pipelined_write = (GetParam() & 1) != 0;
    options.async_commit_memtable_threads = 1 << (GetParam() / 4);
    options.allow_concurrent_memtable_write =
        options.async_commit_memtable_threads > 1;
    return options;
  }
  WriteOptions Writes() {
    WriteOptions options;
    options.sync = (GetParam() & 2) != 0;
    options.memtable_insert_hint_per_batch = true;
    return options;
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
  Reopen(PoolOptions());
  PoolEvent applying, release_apply, second_wal, release_wal;
  PoolEvent completing, release_completion, acknowledged;
  std::atomic<int> wal_count{0};
  std::atomic<bool> first_completion{true};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    if (WriteBatchInternal::Sequence(static_cast<WriteBatch*>(arg)) == 1) {
      applying.Set();
      EXPECT_TRUE(release_apply.Wait());
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
  release_completion.Set();
  second.join();
  EXPECT_TRUE(independent);
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, PollingFindsWorkWithoutWake) {
  Options options = PoolOptions();
  options.async_commit_memtable_threads = 1;
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
  options.async_commit_memtable_threads = 1;
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

TEST_P(AsyncCommitExecutorTest, WALHandoffTimingFollowsSyncPolicy) {
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
  if (Writes().sync) {
    EXPECT_FALSE(second_wal.IsSet());
  } else {
    EXPECT_TRUE(second_wal.Wait(std::chrono::seconds(1)));
  }
  release_notify.Set();
  first.join();
  second.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  ExpectDrained();
}

TEST_P(AsyncCommitExecutorTest, OptionsAndConcurrentFactoryValidation) {
  Options options = PoolOptions();
  Options invalid = options;
  invalid.async_commit_memtable_threads = -1;
  EXPECT_TRUE(TryReopen(invalid).IsInvalidArgument());
  invalid = options;
  invalid.enable_async_commit = false;
  EXPECT_TRUE(TryReopen(invalid).IsInvalidArgument());
  invalid = options;
  invalid.max_async_commit_pending_groups = 0;
  EXPECT_TRUE(TryReopen(invalid).IsInvalidArgument());
  invalid = options;
  invalid.async_commit_memtable_threads = 2;
  invalid.allow_concurrent_memtable_write = false;
  EXPECT_TRUE(TryReopen(invalid).IsInvalidArgument());
  Reopen(options);
  if (options.async_commit_memtable_threads > 1) {
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
  options.max_async_commit_pending_groups = 1;
  options.max_async_commit_pending_bytes = 1;
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
  if (options.async_commit_memtable_threads < 2) return;
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

TEST_P(AsyncCommitExecutorTest, OneGroupRunsRangesOnDifferentWorkers) {
  Options options = PoolOptions();
  if (options.async_commit_memtable_threads < 2) return;
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
  EXPECT_TRUE(second_range.Wait());
  EXPECT_TRUE(follower_ack.Wait());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(leader_ack.Wait());
  release.Set();
  leader.join();
  follower.join();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
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
  if (options.async_commit_memtable_threads > 1) {
    EXPECT_TRUE(fence1.Wait());
  }
  EXPECT_FALSE(merged.IsSet());
  EXPECT_FALSE(later.IsSet());
  release_first.Set();
  EXPECT_TRUE(merged.Wait());
  EXPECT_OK(db_->Put(Writes(), "last", "four"));
  if (options.async_commit_memtable_threads > 1) {
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
  options.enable_async_commit = true;
  options.enable_pipelined_write = (mode & 1) != 0;
  options.async_commit_memtable_threads = 1 << (mode / 4);
  options.allow_concurrent_memtable_write =
      options.async_commit_memtable_threads > 1;
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
    options.enable_async_commit = reopen != 0;
    options.async_commit_memtable_threads = reopen == 2 ? 2 : 0;
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
  options.max_async_commit_pending_groups = 1;
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
  options.enable_async_commit = false;
  options.async_commit_memtable_threads = 0;
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
                        testing::Range(0, 12));
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
