// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#if defined(__linux__)
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "db/write_thread.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

class AsyncCommitTest : public DBTestBase {
 public:
  AsyncCommitTest() : DBTestBase("async_commit_test", true) {}
};

TEST_F(AsyncCommitTest, PoolOptionsRoundTrip) {
  DBOptions parsed;
  ASSERT_OK(GetDBOptionsFromString(
      DBOptions(),
      "enable_async_commit=true;async_commit_memtable_threads=1;"
      "max_async_commit_pending_groups=17;", &parsed));
  Options options(parsed, ColumnFamilyOptions());
  options.create_if_missing = true;
  options.allow_concurrent_memtable_write = false;
  Reopen(options);
  DBOptions loaded;
  std::vector<ColumnFamilyDescriptor> columns;
  ASSERT_OK(LoadLatestOptions(dbname_, env_, &loaded, &columns));
  std::string serialized;
  ASSERT_OK(GetStringFromDBOptions(&serialized, loaded));
  ASSERT_NE(std::string::npos,
            serialized.find("async_commit_memtable_threads=1"));
  ASSERT_NE(std::string::npos,
            serialized.find("max_async_commit_pending_groups=17"));
  EXPECT_FALSE(db_->SetDBOptions({{"async_commit_memtable_threads", "2"}}).ok());
  EXPECT_FALSE(db_->SetDBOptions({{"max_async_commit_pending_groups", "1"}}).ok());
}

TEST_F(AsyncCommitTest, OptionsRoundTrip) {
  DBOptions parsed;
  ASSERT_OK(GetDBOptionsFromString(
      DBOptions(),
      "enable_async_commit=true;max_async_commit_pending_bytes=67108864;",
      &parsed));
  Options options(parsed, ColumnFamilyOptions());
  options.create_if_missing = true;
  Reopen(options);
  DBOptions loaded;
  std::vector<ColumnFamilyDescriptor> columns;
  ASSERT_OK(LoadLatestOptions(dbname_, env_, &loaded, &columns));
  ASSERT_TRUE(loaded.enable_async_commit);
  ASSERT_EQ(67108864U, loaded.max_async_commit_pending_bytes);
  EXPECT_FALSE(db_->SetDBOptions({{"enable_async_commit", "false"}}).ok());
  EXPECT_FALSE(
      db_->SetDBOptions({{"max_async_commit_pending_bytes", "1"}}).ok());
  std::string serialized;
  ASSERT_OK(GetStringFromDBOptions(&serialized, dbfull()->GetDBOptions()));
  ASSERT_NE(serialized.find("enable_async_commit=true"), std::string::npos);
  ASSERT_NE(serialized.find("max_async_commit_pending_bytes=67108864"),
            std::string::npos);
}

TEST_F(AsyncCommitTest, RejectIncompatibleOpenAndTransactionWrappers) {
  Options options;
  options.create_if_missing = true;
  options.enable_async_commit = true;
  for (int which = 0; which < 5; ++which) {
    Options invalid = options;
    if (which == 0) invalid.unordered_write = true;
    if (which == 1) invalid.two_write_queues = true;
    if (which == 2) invalid.manual_wal_flush = true;
    if (which == 3) invalid.allow_2pc = true;
    if (which == 4) invalid.max_async_commit_pending_bytes = 0;
    EXPECT_FALSE(TryReopen(invalid).ok());
  }
  TransactionDB* pessimistic = nullptr;
  EXPECT_TRUE(TransactionDB::Open(options, TransactionDBOptions(), dbname_,
                                  &pessimistic)
                  .IsNotSupported());
  EXPECT_EQ(nullptr, pessimistic);
  OptimisticTransactionDB* optimistic = nullptr;
  EXPECT_TRUE(OptimisticTransactionDB::Open(options, dbname_, &optimistic)
                  .IsNotSupported());
  EXPECT_EQ(nullptr, optimistic);
  Reopen(options);
}

TEST_F(AsyncCommitTest, ExplicitOptimisticOptionsRejected) {
  Close();
  Options options;
  options.create_if_missing = true;
  options.enable_async_commit = true;
  std::vector<ColumnFamilyHandle*> handles;
  OptimisticTransactionDB* optimistic = nullptr;
  Status status = OptimisticTransactionDB::Open(
      DBOptions(options), OptimisticTransactionDBOptions(), dbname_,
      {ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions())},
      &handles, &optimistic);
  EXPECT_TRUE(status.IsNotSupported());
  if (optimistic) {
    for (auto* handle : handles)
      EXPECT_OK(optimistic->DestroyColumnFamilyHandle(handle));
    delete optimistic;
  }
  Reopen(options);
}

TEST_F(AsyncCommitTest, EmptyRangeSequenceSurvivesOptionChanges) {
  for (bool initial_async : {false, true}) {
    Options options;
    options.env = env_;
    options.create_if_missing = true;
    options.enable_async_commit = initial_async;
    options.avoid_flush_during_shutdown = true;
    DestroyAndReopen(options);
    WriteBatch batch;
    ASSERT_OK(batch.DeleteRange("x", "x"));
    ASSERT_OK(batch.Put("key", "value"));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_EQ(2U, db_->GetLatestSequenceNumber());
    options.enable_async_commit = !initial_async;
    Reopen(options);
    EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
    EXPECT_EQ("value", Get("key"));
    ASSERT_OK(db_->Put(WriteOptions(), "next", "value"));
    EXPECT_EQ(3U, db_->GetLatestSequenceNumber());
  }
}

TEST_F(AsyncCommitTest, TrailingMetadataPreservesRecoverySequence) {
  for (int mode = 0; mode < 3; ++mode) {
    Options options;
    options.env = env_;
    options.create_if_missing = true;
    options.enable_async_commit = mode != 0;
    options.enable_pipelined_write = true;
    options.async_commit_memtable_threads = mode == 2 ? 1 : 0;
    options.allow_concurrent_memtable_write = false;
    options.avoid_flush_during_shutdown = true;
    DestroyAndReopen(options);
    ASSERT_OK(db_->Put(WriteOptions(), "last", "acknowledged"));
    WriteBatch metadata;
    ASSERT_OK(db_->Write(WriteOptions(), &metadata));
    metadata.PutLogData("trailing metadata");
    ASSERT_OK(db_->Write(WriteOptions(), &metadata));
    ASSERT_OK(dbfull()->WaitForAsyncCommit());
    EXPECT_EQ(1U, db_->GetLatestSequenceNumber());
    options.enable_async_commit = false;
    options.async_commit_memtable_threads = 0;
    Reopen(options);
    EXPECT_EQ(1U, db_->GetLatestSequenceNumber());
    EXPECT_EQ("acknowledged", Get("last"));
  }
}

// Events control interleavings; timeouts only prevent a broken test from
// hanging.
class AsyncEvent {
 public:
  void Set() {
    std::lock_guard<std::mutex> lock(mu_);
    ready_ = true;
    cv_.notify_all();
  }
  bool Wait() {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_until(
        lock, std::chrono::system_clock::now() + std::chrono::seconds(15),
        [&] { return ready_; });
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool ready_ = false;
};

TEST_F(AsyncCommitTest, PoolOwnsAcknowledgedBatchAndCloseDrains) {
  Options options;
  options.create_if_missing = true;
  options.enable_async_commit = true;
  options.enable_pipelined_write = true;
  options.allow_concurrent_memtable_write = false;
  options.async_commit_memtable_threads = 1;
  options.avoid_flush_during_shutdown = true;
  Reopen(options);
  AsyncEvent applying, release, acknowledged, closing, closed;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
        applying.Set();
        EXPECT_TRUE(release.Wait());
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::thread writer([&] {
    WriteBatch batch(0, 0, 8);
    EXPECT_OK(batch.Put("owned", std::string(1024, 'v')));
    EXPECT_OK(db_->Write(WriteOptions(), &batch));
    batch.Clear();
    EXPECT_OK(batch.Put("clobbered", std::string(1024, 'x')));
    acknowledged.Set();
  });
  EXPECT_TRUE(applying.Wait());
  bool early_ack = acknowledged.Wait();
  uint64_t pending = 0;
  EXPECT_TRUE(db_->GetIntProperty("rocksdb.async-commit-pending-groups", &pending));
  EXPECT_EQ(1U, pending);
  std::thread closer([&] {
    closing.Set();
    EXPECT_OK(db_->Close());
    closed.Set();
  });
  EXPECT_TRUE(closing.Wait());
  release.Set();
  writer.join();
  closer.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_TRUE(early_ack);
  EXPECT_TRUE(closed.Wait());
  Reopen(options);
  EXPECT_EQ(std::string(1024, 'v'), Get("owned"));
  EXPECT_EQ("NOT_FOUND", Get("clobbered"));
}

TEST_F(AsyncCommitTest, PoolOutOfOrderCompletionPublishesOnlyWholePrefix) {
  Options options;
  options.create_if_missing = true;
  options.enable_async_commit = true;
  options.enable_pipelined_write = true;
  options.allow_concurrent_memtable_write = true;
  options.async_commit_memtable_threads = 2;
  Reopen(options);
  CreateAndReopenWithCF({"second"}, options);
  AsyncEvent first_started, release_first, later_applied;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    auto* batch = static_cast<WriteBatch*>(arg);
    if (WriteBatchInternal::Sequence(batch) == 1) {
      first_started.Set();
      EXPECT_TRUE(release_first.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void*) {
    later_applied.Set();
  });
  sp->EnableProcessing();
  WriteBatch first(0, 0, 8);
  ASSERT_OK(first.Put(handles_[0], "same", "old"));
  ASSERT_OK(first.Put(handles_[1], "same", "old"));
  ASSERT_OK(db_->Write(WriteOptions(), &first));
  EXPECT_TRUE(first_started.Wait());
  WriteBatch second(0, 0, 8);
  ASSERT_OK(second.Put(handles_[0], "same", "new"));
  ASSERT_OK(second.Put(handles_[1], "same", "new"));
  ASSERT_OK(db_->Write(WriteOptions(), &second));
  EXPECT_TRUE(later_applied.Wait());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  std::thread observer([&] {
    std::string value;
    EXPECT_TRUE(db_->Get(ReadOptions(), handles_[0], "same", &value).IsNotFound());
    EXPECT_TRUE(db_->Get(ReadOptions(), handles_[1], "same", &value).IsNotFound());
  });
  observer.join();
  ReadOptions deadline;
  deadline.deadline = std::chrono::microseconds(env_->NowMicros() + 10000);
  std::string value;
  EXPECT_TRUE(db_->Get(deadline, handles_[0], "same", &value).IsTimedOut());
  release_first.Set();
  EXPECT_OK(dbfull()->WaitForAsyncCommit());
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  EXPECT_EQ(4U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("new", Get(0, "same"));
  EXPECT_EQ("new", Get(1, "same"));
}

class AsyncCommitModesTest : public AsyncCommitTest,
                             public testing::WithParamInterface<int> {
 public:
  Options AsyncOptions() {
    DBOptions parsed;
    EXPECT_OK(GetDBOptionsFromString(
        DBOptions(),
        "enable_async_commit=true;enable_pipelined_write=" +
            std::string((GetParam() & 1) ? "true" : "false") +
            ";allow_concurrent_memtable_write=" +
            std::string((GetParam() & 2) ? "true" : "false"),
        &parsed));
    Options options(parsed, ColumnFamilyOptions());
    options.env = env_;
    options.create_if_missing = true;
    return options;
  }
  WriteOptions Writes() {
    WriteOptions wo;
    wo.sync = (GetParam() & 4) != 0;
    return wo;
  }
  void RunGroup(const std::function<void()>& leader_fn,
                const std::function<void()>& follower_fn) {
    AsyncEvent leader_ready, follower_queued, form;
    auto* sp = SyncPoint::GetInstance();
    sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeGroup", [&](void*) {
      leader_ready.Set();
      EXPECT_TRUE(form.Wait());
    });
    sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
      auto* writer = static_cast<WriteThread::Writer*>(arg);
      if (writer->state.load() != WriteThread::STATE_GROUP_LEADER) {
        follower_queued.Set();
      }
    });
    sp->EnableProcessing();
    std::thread leader(leader_fn);
    EXPECT_TRUE(leader_ready.Wait());
    std::thread follower(follower_fn);
    EXPECT_TRUE(follower_queued.Wait());
    form.Set();
    leader.join();
    follower.join();
    sp->DisableProcessing();
  }
  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    AsyncCommitTest::TearDown();
  }
};

TEST_P(AsyncCommitModesTest, RejectUnsupportedWritesBeforeWAL) {
  Reopen(AsyncOptions());
  WriteOptions wo = Writes();
  wo.disableWAL = true;
  Status invalid = db_->Put(wo, "bad", "value");
  EXPECT_TRUE(wo.sync ? invalid.IsInvalidArgument() : invalid.IsNotSupported());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  WriteBatch transaction;
  ASSERT_OK(WriteBatchInternal::MarkCommit(&transaction, "txn"));
  EXPECT_TRUE(db_->Write(Writes(), &transaction).IsNotSupported());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  ASSERT_OK(db_->Put(Writes(), "valid", "value"));
  EXPECT_EQ("value", Get("valid"));
  ReadOptions tailing;
  tailing.tailing = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(tailing));
  EXPECT_TRUE(iter->status().IsNotSupported());
}

TEST_P(AsyncCommitModesTest, RejectUnsupportedCFOptions) {
  Reopen(AsyncOptions());
  EXPECT_FALSE(db_->SetOptions({{"max_successive_merges", "2"}}).ok());
  ColumnFamilyOptions cf;
  cf.max_successive_merges = 2;
  ColumnFamilyHandle* handle = nullptr;
  EXPECT_FALSE(db_->CreateColumnFamily(cf, "bad", &handle).ok());
  EXPECT_EQ(nullptr, handle);
  cf.max_successive_merges = 0;
  ASSERT_OK(db_->CreateColumnFamily(cf, "good", &handle));
  ASSERT_OK(db_->Put(Writes(), handle, "key", "value"));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
}

// Losing the WAL return boundary, retaining caller payload, failing to record
// the batch END sequence, or obtaining a view before waiting breaks this test.
TEST_P(AsyncCommitModesTest, FollowerOwnershipAndReadViews) {
  Reopen(AsyncOptions());
  ASSERT_OK(db_->Put(Writes(), "old", "before"));
  const Snapshot* historical = db_->GetSnapshot();
  AsyncEvent leader_ready, follower_queued, form_group, apply_ready, apply,
      returned, read_waiting;
  std::atomic<bool> leader_done{false}, final_read{false};
  auto* sp = SyncPoint::GetInstance();
  const char* wal_data = nullptr;
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeGroup", [&](void*) {
    leader_ready.Set();
    EXPECT_TRUE(form_group.Wait());
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    auto* w = static_cast<WriteThread::Writer*>(arg);
    if (WriteBatchInternal::Count(w->batch) == 2) follower_queued.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void* arg) {
    wal_data =
        WriteBatchInternal::Contents(static_cast<WriteBatch*>(arg)).data();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    EXPECT_EQ(
        wal_data,
        WriteBatchInternal::Contents(static_cast<WriteBatch*>(arg)).data());
    apply_ready.Set();
    EXPECT_TRUE(apply.Wait());
  });
  sp->SetCallBack("DBImpl::PrepareAsyncCommitRead:Wait", [&](void*) {
    if (final_read.load()) read_waiting.Set();
  });
  sp->EnableProcessing();
  std::thread leader([&] {
    EXPECT_OK(db_->Put(Writes(), "leader", "value"));
    leader_done.store(true);
  });
  EXPECT_TRUE(leader_ready.Wait());
  std::thread follower([&] {
    WriteBatch unprotected;
    EXPECT_OK(unprotected.Put("follower", "owned"));
    EXPECT_OK(unprotected.Put("old", "after"));
    EXPECT_OK(db_->Write(Writes(), &unprotected));
    unprotected.Clear();
    EXPECT_OK(unprotected.Put("follower", "caller-reused"));
    returned.Set();
    ReadOptions expired;
    expired.deadline = std::chrono::microseconds(env_->NowMicros() - 1);
    std::string value;
    EXPECT_TRUE(db_->Get(expired, "follower", &value).IsTimedOut());
    ReadOptions deadline;
    deadline.deadline = std::chrono::microseconds(env_->NowMicros() + 5000);
    EXPECT_TRUE(db_->Get(deadline, "follower", &value).IsTimedOut());
    bool found = true;
    EXPECT_TRUE(db_->KeyMayExist(expired, db_->DefaultColumnFamily(),
                                 "follower", &value, &found));
    EXPECT_FALSE(found);
    std::vector<std::string> values;
    auto statuses = db_->MultiGet(
        expired, {db_->DefaultColumnFamily(), db_->DefaultColumnFamily()},
        {Slice("follower"), Slice("old")}, &values);
    for (const auto& status : statuses) EXPECT_TRUE(status.IsTimedOut());
    std::unique_ptr<Iterator> error_iter(db_->NewIterator(expired));
    EXPECT_TRUE(error_iter->status().IsTimedOut());
    std::vector<Iterator*> iters;
    EXPECT_TRUE(db_->NewIterators(expired, {db_->DefaultColumnFamily()}, &iters)
                    .IsTimedOut());
    EXPECT_TRUE(iters.empty());
    final_read.store(true);
    deadline.deadline = std::chrono::microseconds(env_->NowMicros() + 10000000);
    EXPECT_OK(db_->Get(deadline, "follower", &value));
    EXPECT_EQ("owned", value);
    ReadOptions old;
    old.snapshot = historical;
    EXPECT_OK(db_->Get(old, "old", &value));
    EXPECT_EQ("before", value);
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek("follower");
    EXPECT_TRUE(iter->Valid());
    if (iter->Valid()) {
      EXPECT_EQ("owned", iter->value().ToString());
    }
  });
  EXPECT_TRUE(follower_queued.Wait());
  form_group.Set();
  EXPECT_TRUE(apply_ready.Wait());
  EXPECT_TRUE(returned.Wait());
  EXPECT_FALSE(leader_done.load());
  std::string external;
  EXPECT_TRUE(db_->Get(ReadOptions(), "follower", &external).IsNotFound());
  EXPECT_EQ(1U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(read_waiting.Wait());
  apply.Set();
  follower.join();
  leader.join();
  sp->DisableProcessing();
  EXPECT_EQ(4U, db_->GetLatestSequenceNumber());
  db_->ReleaseSnapshot(historical);
}

TEST_P(AsyncCommitModesTest, EmptyLogDataProtectedBatchesAndReopen) {
  Reopen(AsyncOptions());
  WriteBatch empty;
  ASSERT_OK(db_->Write(Writes(), &empty));
  ASSERT_EQ(0U, db_->GetLatestSequenceNumber());
  empty.PutLogData("metadata");
  ASSERT_OK(db_->Write(Writes(), &empty));
  ASSERT_EQ(0U, db_->GetLatestSequenceNumber());
  WriteBatch batch(0, 0, 8);
  ASSERT_OK(batch.Put("a", std::string(70000, 'a')));
  batch.PutLogData("between");
  ASSERT_OK(batch.Put("b", "bee"));
  ASSERT_OK(batch.DeleteRange("c", "c"));
  ASSERT_OK(batch.Put("d", "dee"));
  ASSERT_OK(db_->Write(Writes(), &batch));
  ASSERT_EQ(4U, db_->GetLatestSequenceNumber());
  ASSERT_EQ("dee", Get("d"));
  Reopen(AsyncOptions());
  ASSERT_EQ(std::string(70000, 'a'), Get("a"));
  ASSERT_EQ("bee", Get("b"));
  ASSERT_EQ("dee", Get("d"));
}

TEST_P(AsyncCommitModesTest, ThreadedWritesFlushAndRecovery) {
  Options options = AsyncOptions();
  options.write_buffer_size = 64 * 1024;
  Reopen(options);
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      for (int n = 0; n < 150; ++n) {
        std::string key = std::to_string(t) + ":" + std::to_string(n);
        WriteBatch batch;
        EXPECT_OK(batch.Put(key, std::string(1000, 'v')));
        EXPECT_OK(batch.Put(key + ":pair", "paired"));
        EXPECT_OK(db_->Write(Writes(), &batch));
        batch.Clear();
        std::string value;
        EXPECT_OK(db_->Get(ReadOptions(), key + ":pair", &value));
        EXPECT_EQ("paired", value);
      }
    });
  }
  for (auto& thread : threads) thread.join();
  ASSERT_EQ(2400U, db_->GetLatestSequenceNumber());
  ASSERT_OK(Flush());
  Reopen(options);
  for (int t = 0; t < 8; ++t) {
    ASSERT_EQ("paired", Get(std::to_string(t) + ":149:pair"));
  }
}

TEST_P(AsyncCommitModesTest, ProtectedRangesMissingCFAndLogData) {
  Reopen(AsyncOptions());
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    auto* batch = static_cast<WriteBatch*>(arg);
    // Reject a shifted first-record index, a middle-of-record offset and
    // a wrong count, without writing anything to a memtable.
    EXPECT_TRUE(WriteBatchInternal::ValidateAsyncCommitRange(
                    batch, 12, WriteBatchInternal::ByteSize(batch), 1, 3)
                    .IsCorruption());
    EXPECT_TRUE(WriteBatchInternal::ValidateAsyncCommitRange(
                    batch, 13, WriteBatchInternal::ByteSize(batch), 0, 4)
                    .IsCorruption());
    EXPECT_TRUE(WriteBatchInternal::ValidateAsyncCommitRange(
                    batch, 12, WriteBatchInternal::ByteSize(batch), 0, 3)
                    .IsCorruption());
  });
  RunGroup(
      [&] {
        WriteBatch batch(0, 0, 8);
        EXPECT_OK(batch.Put("a", "aye"));
        batch.PutLogData("leader metadata");
        EXPECT_OK(batch.Put("b", "bee"));
        EXPECT_OK(db_->Write(Writes(), &batch));
      },
      [&] {
        WriteBatch batch(0, 0, 8);
        EXPECT_OK(WriteBatchInternal::Put(&batch, 99999, "missing", "ignored"));
        EXPECT_OK(batch.Put("z", std::string(70000, 'z')));
        batch.PutLogData("follower metadata");
        WriteOptions wo = Writes();
        wo.ignore_missing_column_families = true;
        EXPECT_OK(db_->Write(wo, &batch));
        batch.Clear();
        std::string value;
        EXPECT_OK(db_->Get(ReadOptions(), "z", &value));
        EXPECT_EQ(std::string(70000, 'z'), value);
      });
  EXPECT_EQ(4U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("aye", Get("a"));
  EXPECT_EQ("bee", Get("b"));
}

TEST_P(AsyncCommitModesTest, ApplicationFailureStopsPublicationAndReads) {
  Reopen(AsyncOptions());
  AsyncEvent read_waiting;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::PrepareAsyncCommitRead:Wait",
                  [&](void*) { read_waiting.Set(); });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply",
                  [&](void*) { EXPECT_TRUE(read_waiting.Wait()); });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:ApplyStatus", [&](void* arg) {
    *static_cast<Status*>(arg) = Status::IOError("injected memtable failure");
  });
  RunGroup(
      [&] { EXPECT_TRUE(db_->Put(Writes(), "leader", "value").IsIOError()); },
      [&] {
        EXPECT_OK(db_->Put(Writes(), "follower", "value"));
        std::string value;
        EXPECT_TRUE(db_->Get(ReadOptions(), "follower", &value).IsIOError());
        EXPECT_EQ(nullptr, db_->GetSnapshot());
        bool found = true;
        EXPECT_TRUE(db_->KeyMayExist(ReadOptions(), db_->DefaultColumnFamily(),
                                     "follower", &value, &found));
        EXPECT_FALSE(found);
        std::vector<std::string> values;
        auto statuses =
            db_->MultiGet(ReadOptions(), {db_->DefaultColumnFamily()},
                          {Slice("follower")}, &values);
        EXPECT_TRUE(statuses[0].IsIOError());
      });
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(db_->Put(Writes(), "later", "no").IsIOError());
  EXPECT_TRUE(db_->Resume().IsIOError());
  EXPECT_TRUE(db_->Flush(FlushOptions()).IsIOError());
  ColumnFamilyHandle* rejected = nullptr;
  EXPECT_TRUE(
      db_->CreateColumnFamily(ColumnFamilyOptions(), "after-error", &rejected)
          .IsIOError());
  if (rejected) {
    EXPECT_OK(db_->DropColumnFamily(rejected));
    EXPECT_OK(db_->DestroyColumnFamilyHandle(rejected));
  }
  sp->ClearAllCallBacks();
  Reopen(AsyncOptions());
  EXPECT_EQ("value", Get("follower"));
  EXPECT_EQ("value", Get("leader"));
  EXPECT_OK(db_->Put(Writes(), "reopened", "yes"));
}

TEST_P(AsyncCommitModesTest, WALFailureAcknowledgesNobodyAndDrains) {
  Reopen(AsyncOptions());
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AsyncWriteImpl:WALStatus", [&](void* arg) {
        *static_cast<IOStatus*>(arg) =
            IOStatus::IOError("injected WAL failure");
      });
  RunGroup(
      [&] { EXPECT_TRUE(db_->Put(Writes(), "leader", "value").IsIOError()); },
      [&] {
        EXPECT_TRUE(db_->Put(Writes(), "follower", "value").IsIOError());
      });
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(db_->Put(Writes(), "later", "no").IsIOError());
}

TEST_P(AsyncCommitModesTest, DBAndThreadLifetimeIsolation) {
  Options options = AsyncOptions();
  Reopen(options);
  const std::string other_name = dbname_ + "-other";
  ASSERT_OK(DestroyDB(other_name, options));
  DB* other = nullptr;
  ASSERT_OK(DB::Open(options, other_name, &other));
  ASSERT_OK(db_->Put(Writes(), "first", "one"));
  std::string value;
  ASSERT_TRUE(other->Get(ReadOptions(), "absent", &value).IsNotFound());
  ASSERT_EQ(0U, other->GetLatestSequenceNumber());
  delete other;
  ASSERT_OK(DestroyDB(other_name, options));
  DestroyAndReopen(options);
  ASSERT_TRUE(db_->Get(ReadOptions(), "first", &value).IsNotFound());
  ASSERT_EQ(0U, db_->GetLatestSequenceNumber());
  ASSERT_OK(db_->Put(Writes(), "fresh", "two"));
}

TEST_P(AsyncCommitModesTest, PipelinedOriginalGroupsOverlapInOrder) {
  if (!(GetParam() & 1)) return;
  Reopen(AsyncOptions());
  AsyncEvent applying_first, release_first, second_leader_ready,
      second_follower_queued, form_second, second_wal, second_returned;
  std::atomic<int> groups{0}, applications{0};
  std::mutex owner_mutex;
  std::map<WriteBatch*, std::thread::id> owners;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeGroup", [&](void*) {
    if (++groups == 2) {
      second_leader_ready.Set();
      EXPECT_TRUE(form_second.Wait());
    }
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    auto* w = static_cast<WriteThread::Writer*>(arg);
    if (w->state.load() != WriteThread::STATE_GROUP_LEADER)
      second_follower_queued.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void* arg) {
    std::lock_guard<std::mutex> lock(owner_mutex);
    owners[static_cast<WriteBatch*>(arg)] = std::this_thread::get_id();
    if (groups.load() == 2) second_wal.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void* arg) {
    {
      std::lock_guard<std::mutex> lock(owner_mutex);
      EXPECT_EQ(owners[static_cast<WriteBatch*>(arg)],
                std::this_thread::get_id());
    }
    if (++applications == 1) {
      applying_first.Set();
      EXPECT_TRUE(release_first.Wait());
    }
  });
  sp->EnableProcessing();
  std::thread first([&] { EXPECT_OK(db_->Put(Writes(), "first", "one")); });
  EXPECT_TRUE(applying_first.Wait());
  std::thread second([&] { EXPECT_OK(db_->Put(Writes(), "second", "two")); });
  EXPECT_TRUE(second_leader_ready.Wait());
  std::thread follower([&] {
    EXPECT_OK(db_->Put(Writes(), "third", "three"));
    second_returned.Set();
    std::string value;
    EXPECT_OK(db_->Get(ReadOptions(), "third", &value));
    EXPECT_EQ("three", value);
  });
  EXPECT_TRUE(second_follower_queued.Wait());
  form_second.Set();
  EXPECT_TRUE(second_wal.Wait());
  EXPECT_TRUE(second_returned.Wait());
  EXPECT_EQ(1, applications.load());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  release_first.Set();
  first.join();
  second.join();
  follower.join();
  sp->DisableProcessing();
  EXPECT_EQ(2, applications.load());
  EXPECT_EQ(3U, db_->GetLatestSequenceNumber());
}

TEST_P(AsyncCommitModesTest, EmptyWALQueueConcurrentEnqueue) {
  if (!(GetParam() & 1)) return;
  Reopen(AsyncOptions());
  AsyncEvent boundary, queued, handoff, applying, release, second_wal;
  std::atomic<int> boundaries{0}, wal_groups{0}, apply_groups{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("WriteThread::TransferAsyncGroupToMemtable:Boundary",
                  [&](void*) {
                    if (++boundaries == 1) {
                      boundary.Set();
                      EXPECT_TRUE(handoff.Wait());
                    }
                  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    auto* w = static_cast<WriteThread::Writer*>(arg);
    if (w->state.load() != WriteThread::STATE_GROUP_LEADER) queued.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void*) {
    if (++wal_groups == 2) second_wal.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    if (++apply_groups == 1) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  sp->EnableProcessing();
  std::thread first([&] { EXPECT_OK(db_->Put(Writes(), "first", "one")); });
  EXPECT_TRUE(boundary.Wait());
  std::thread second([&] { EXPECT_OK(db_->Put(Writes(), "second", "two")); });
  EXPECT_TRUE(queued.Wait());
  handoff.Set();
  EXPECT_TRUE(applying.Wait());
  EXPECT_TRUE(second_wal.Wait());
  EXPECT_EQ(1, apply_groups.load());
  release.Set();
  first.join();
  second.join();
  sp->DisableProcessing();
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("one", Get("first"));
  EXPECT_EQ("two", Get("second"));
}

TEST_P(AsyncCommitModesTest, PendingCapacityAndNoSlowdown) {
  if (!(GetParam() & 1)) return;
  Options options = AsyncOptions();
  options.max_async_commit_pending_bytes = 1;  // Metadata alone exceeds this.
  Reopen(options);
  AsyncEvent applying, release, capacity_wait;
  std::atomic<int> applications{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    if (++applications == 1) {
      applying.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:CapacityWait",
                  [&](void*) { capacity_wait.Set(); });
  sp->EnableProcessing();
  std::thread first([&] { EXPECT_OK(db_->Put(Writes(), "first", "one")); });
  EXPECT_TRUE(applying.Wait());
  WriteOptions wo = Writes();
  wo.no_slowdown = true;
  EXPECT_TRUE(db_->Put(wo, "rejected", "no").IsIncomplete());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  std::thread pending([&] { EXPECT_OK(db_->Put(Writes(), "second", "two")); });
  EXPECT_TRUE(capacity_wait.Wait());
  release.Set();
  first.join();
  pending.join();
  sp->DisableProcessing();
  uint64_t bytes = 99, groups_count = 99;
  EXPECT_TRUE(
      db_->GetIntProperty("rocksdb.async-commit-pending-bytes", &bytes));
  EXPECT_TRUE(db_->GetIntProperty("rocksdb.async-commit-pending-groups",
                                  &groups_count));
  EXPECT_EQ(0U, bytes);
  EXPECT_EQ(0U, groups_count);
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_EQ("two", Get("second"));
}

TEST_P(AsyncCommitModesTest, AtomicCrossCFViewsAndIteratorRefresh) {
  Reopen(AsyncOptions());
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "second", &cf));
  ASSERT_OK(db_->Put(Writes(), "key", "old-default"));
  ASSERT_OK(db_->Put(Writes(), cf, "key", "old-second"));
  AsyncEvent partial, release, refreshing;
  std::atomic<int> inserted{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("MemTable::Add:Encoded", [&](void*) {
    if (++inserted == 2) {
      partial.Set();
      EXPECT_TRUE(release.Wait());
    }
  });
  std::atomic<bool> final_refresh{false};
  sp->SetCallBack("DBImpl::PrepareAsyncCommitRead:Wait", [&](void*) {
    if (final_refresh.load()) refreshing.Set();
  });
  // RunGroup's control thread must inspect the partial group while both write
  // calls are active, so use it from a separate harness thread.
  std::thread harness([&] {
    RunGroup(
        [&] {
          WriteBatch batch(0, 0, 8);
          EXPECT_OK(batch.Put("key", "new-default"));
          EXPECT_OK(batch.Put(cf, "key", "new-second"));
          EXPECT_OK(db_->Write(Writes(), &batch));
        },
        [&] {
          ReadOptions expired;
          expired.deadline = std::chrono::microseconds(env_->NowMicros() - 1);
          std::unique_ptr<Iterator> old(db_->NewIterator(expired));
          std::unique_ptr<Iterator> renewable(db_->NewIterator(ReadOptions()));
          WriteBatch batch(0, 0, 8);
          EXPECT_OK(batch.Put("tail", "last"));
          EXPECT_OK(db_->Write(Writes(), &batch));
          EXPECT_TRUE(old->Refresh().IsTimedOut());
          old->Seek("key");
          EXPECT_TRUE(old->Valid());
          if (old->Valid()) {
            EXPECT_EQ("old-default", old->value().ToString());
          }
          final_refresh.store(true);
          EXPECT_OK(renewable->Refresh());
          renewable->Seek("key");
          EXPECT_TRUE(renewable->Valid());
          if (renewable->Valid()) {
            EXPECT_EQ("new-default", renewable->value().ToString());
          }
        });
  });
  EXPECT_TRUE(partial.Wait());
  EXPECT_TRUE(refreshing.Wait());
  const Snapshot* snapshot = db_->GetSnapshot();
  EXPECT_NE(nullptr, snapshot);
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  std::vector<std::string> values;
  auto statuses = db_->MultiGet(ReadOptions(), {db_->DefaultColumnFamily(), cf},
                                {Slice("key"), Slice("key")}, &values);
  for (const auto& status : statuses) EXPECT_OK(status);
  EXPECT_EQ((std::vector<std::string>{"old-default", "old-second"}), values);
  std::vector<Iterator*> iterators;
  EXPECT_OK(db_->NewIterators(ReadOptions(), {db_->DefaultColumnFamily(), cf},
                              &iterators));
  for (size_t i = 0; i < iterators.size(); ++i) {
    iterators[i]->Seek("key");
    EXPECT_TRUE(iterators[i]->Valid());
    if (iterators[i]->Valid()) {
      EXPECT_EQ(i == 0 ? "old-default" : "old-second",
                iterators[i]->value().ToString());
    }
    delete iterators[i];
  }
  release.Set();
  harness.join();
  sp->DisableProcessing();
  EXPECT_EQ(5U, db_->GetLatestSequenceNumber());
  ReadOptions historical;
  historical.snapshot = snapshot;
  std::string value;
  EXPECT_OK(db_->Get(historical, cf, "key", &value));
  EXPECT_EQ("old-second", value);
  db_->ReleaseSnapshot(snapshot);
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
}

TEST_P(AsyncCommitModesTest, RealWALAppendAndSyncFailures) {
  Reopen(AsyncOptions());
  env_->log_write_error_.store(true);
  EXPECT_TRUE(db_->Put(Writes(), "append-error", "no").IsIOError());
  env_->log_write_error_.store(false);
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  EXPECT_TRUE(db_->Put(Writes(), "after-error", "no").IsIOError());
  Reopen(AsyncOptions());
  // SpecialEnv's WAL Sync appends corrupt bytes and returns IOError. It is an
  // actual filesystem-layer sync error, not merely a post-WAL status override.
  env_->corrupt_in_sync_ = true;
  WriteOptions wo = Writes();
  wo.sync = true;
  EXPECT_TRUE(db_->Put(wo, "sync-error", "maybe").IsIOError());
  env_->corrupt_in_sync_ = false;
  EXPECT_TRUE(db_->Put(Writes(), "after-sync-error", "no").IsIOError());
}

TEST_P(AsyncCommitModesTest,
       PartialInsertionFailureDrainsLaterGroupsAndBarrier) {
  if (!(GetParam() & 1)) return;
  Reopen(AsyncOptions());
  AsyncEvent partial, fail, later_wal, barrier;
  std::atomic<int> inserted{0}, wal_groups{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:WALComplete", [&](void*) {
    if (++wal_groups == 2) later_wal.Set();
  });
  sp->SetCallBack("MemTable::Add:Encoded", [&](void* arg) {
    if (++inserted == 2) {
      partial.Set();
      EXPECT_TRUE(fail.Wait());
      auto* encoded = static_cast<Slice*>(arg);
      const_cast<char*>(encoded->data())[encoded->size() - 1] ^= 1;
    }
  });
  sp->SetCallBack("WriteThread::AwaitState:BlockingWaiting", [&](void* arg) {
    if (!static_cast<WriteThread::Writer*>(arg)->batch) barrier.Set();
  });
  std::thread harness([&] {
    RunGroup(
        [&] {
          WriteBatch batch(0, 0, 8);
          EXPECT_OK(batch.Put("first", "one"));
          EXPECT_OK(batch.Put("second", "two"));
          EXPECT_TRUE(db_->Write(Writes(), &batch).IsCorruption());
        },
        [&] {
          WriteBatch batch(0, 0, 8);
          EXPECT_OK(batch.Put("follower", "three"));
          EXPECT_OK(db_->Write(Writes(), &batch));
          std::string value;
          EXPECT_TRUE(
              db_->Get(ReadOptions(), "follower", &value).IsCorruption());
        });
  });
  EXPECT_TRUE(partial.Wait());
  std::thread later(
      [&] { EXPECT_TRUE(db_->Put(Writes(), "later", "four").IsCorruption()); });
  EXPECT_TRUE(later_wal.Wait());
  std::thread flush(
      [&] { EXPECT_TRUE(db_->Flush(FlushOptions()).IsCorruption()); });
  EXPECT_TRUE(barrier.Wait());
  fail.Set();
  harness.join();
  later.join();
  flush.join();
  sp->DisableProcessing();
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  uint64_t pending = 99, error = 0;
  EXPECT_TRUE(
      db_->GetIntProperty("rocksdb.async-commit-pending-groups", &pending));
  EXPECT_TRUE(
      db_->GetIntProperty("rocksdb.async-commit-terminal-error", &error));
  EXPECT_EQ(0U, pending);
  EXPECT_EQ(1U, error);
}

TEST_P(AsyncCommitModesTest, ReleasedCFHandleAndDropDrainPendingGroup) {
  Reopen(AsyncOptions());
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "pending", &cf));
  auto* write_handle = new ColumnFamilyHandleImpl(
      static_cast<ColumnFamilyHandleImpl*>(cf)->cfd(), dbfull(),
      dbfull()->mutex());
  AsyncEvent applying, release, acknowledged, dropping;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    applying.Set();
    EXPECT_TRUE(release.Wait());
  });
  sp->SetCallBack("WriteThread::AwaitState:BlockingWaiting", [&](void* arg) {
    if (!static_cast<WriteThread::Writer*>(arg)->batch) dropping.Set();
  });
  std::thread harness([&] {
    RunGroup([&] { EXPECT_OK(db_->Put(Writes(), cf, "leader", "one")); },
             [&] {
               EXPECT_OK(db_->Put(Writes(), write_handle, "follower", "two"));
               EXPECT_OK(db_->DestroyColumnFamilyHandle(write_handle));
               acknowledged.Set();
             });
  });
  EXPECT_TRUE(applying.Wait());
  EXPECT_TRUE(acknowledged.Wait());
  std::thread drop([&] { EXPECT_OK(db_->DropColumnFamily(cf)); });
  EXPECT_TRUE(dropping.Wait());
  EXPECT_EQ(0U, db_->GetLatestSequenceNumber());
  release.Set();
  harness.join();
  drop.join();
  sp->DisableProcessing();
  EXPECT_EQ(2U, db_->GetLatestSequenceNumber());
  EXPECT_OK(db_->DestroyColumnFamilyHandle(cf));
}

TEST_F(AsyncCommitTest, DetachedFollowerReentryAndWriteStall) {
  Options options;
  WriteThread queue{ImmutableDBOptions(options)};
  WriteBatch batch;
  ASSERT_OK(batch.Put("key", "value"));
  WriteThread::Writer leader(WriteOptions(), &batch, nullptr, 0, false);
  queue.JoinBatchGroup(&leader);
  AsyncEvent first_queued, requeued;
  std::atomic<int> followers{0};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    if (static_cast<WriteThread::Writer*>(arg) != &leader) {
      if (++followers == 1)
        first_queued.Set();
      else
        requeued.Set();
    }
  });
  sp->EnableProcessing();
  std::thread follower([&] {
    for (int n = 0; n < 2; ++n) {
      WriteOptions wo;
      wo.no_slowdown = n != 0;
      WriteThread::Writer writer(wo, &batch, nullptr, 0, false);
      queue.JoinBatchGroup(&writer);
      if (n == 0) {
        EXPECT_EQ(WriteThread::STATE_WAL_COMPLETED, writer.state.load());
        EXPECT_OK(writer.FinalStatus());
      } else {
        EXPECT_EQ(WriteThread::STATE_COMPLETED, writer.state.load());
        EXPECT_TRUE(writer.FinalStatus().IsIncomplete());
      }
    }
  });
  EXPECT_TRUE(first_queued.Wait());
  WriteThread::WriteGroup group;
  queue.EnterAsBatchGroupLeader(&leader, &group);
  EXPECT_EQ(2U, group.size);
  queue.DetachAsyncFollowersKeepLeader(group, Status::OK());
  EXPECT_TRUE(requeued.Wait());
  queue.BeginWriteStall();
  queue.EndWriteStall();
  queue.ExitUnbatched(&leader);
  follower.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
}

#if defined(__linux__)
// Executed in a fresh process. It deliberately exits without destructors as
// soon as a follower obtains its WAL acknowledgment, while apply is blocked.
int AsyncCommitCrashWriter(const std::string& path, int mode) {
  Options options;
  options.create_if_missing = true;
  options.enable_async_commit = true;
  options.enable_pipelined_write = (mode & 1) != 0;
  DB* db = nullptr;
  Status opened = DB::Open(options, path, &db);
  if (!opened.ok()) return 10;
  AsyncEvent ready, queued, form;
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeGroup", [&](void*) {
    ready.Set();
    if (!form.Wait()) std::_Exit(11);
  });
  sp->SetCallBack("WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
    if (static_cast<WriteThread::Writer*>(arg)->state.load() !=
        WriteThread::STATE_GROUP_LEADER)
      queued.Set();
  });
  sp->SetCallBack("DBImpl::AsyncWriteImpl:BeforeApply", [&](void*) {
    AsyncEvent never;
    never.Wait();
    std::_Exit(12);
  });
  sp->EnableProcessing();
  WriteOptions wo;
  wo.sync = (mode & 4) != 0;
  std::thread leader([&] {
    db->Put(wo, "leader", "one").PermitUncheckedError();
    std::_Exit(13);
  });
  if (!ready.Wait()) std::_Exit(14);
  std::thread follower([&] {
    Status status = db->Put(wo, "acknowledged", "two");
    std::_Exit(status.ok() ? 0 : 15);
  });
  if (!queued.Wait()) std::_Exit(16);
  form.Set();
  leader.join();
  follower.join();
  return 17;
}

TEST_P(AsyncCommitModesTest, AcknowledgedWALRecoversAfterProcessExit) {
  const std::string path = dbname_ + "-crash";
  Options options = AsyncOptions();
  ASSERT_OK(DestroyDB(path, options));
  std::string mode = std::to_string(GetParam());
  pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    execl("/proc/self/exe", "async_commit_test", "--async-commit-crash-writer",
          path.c_str(), mode.c_str(), static_cast<char*>(nullptr));
    std::_Exit(127);
  }
  int status = 0;
  ASSERT_EQ(child, waitpid(child, &status, 0));
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(0, WEXITSTATUS(status));
  DB* recovered = nullptr;
  ASSERT_OK(DB::Open(options, path, &recovered));
  std::string value;
  EXPECT_OK(recovered->Get(ReadOptions(), "acknowledged", &value));
  EXPECT_EQ("two", value);
  EXPECT_OK(recovered->Get(ReadOptions(), "leader", &value));
  EXPECT_EQ("one", value);
  EXPECT_EQ(2U, recovered->GetLatestSequenceNumber());
  delete recovered;
  ASSERT_OK(DestroyDB(path, options));
}
#endif

INSTANTIATE_TEST_CASE_P(Modes, AsyncCommitModesTest, testing::Range(0, 8));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
#if defined(__linux__)
  if (argc == 4 && std::string(argv[1]) == "--async-commit-crash-writer") {
    return ROCKSDB_NAMESPACE::AsyncCommitCrashWriter(argv[2],
                                                     std::stoi(argv[3]));
  }
#endif
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
