//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Introduction of SyncPoint effectively disabled building and running this test
// in Release build.
// which is a pity, it is a good test
#if !defined(ROCKSDB_LITE)

#include "db/db_test_util.h"
#include "rocksdb/utilities/checkpoint.h"
#include "env/mock_env.h"
#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

class DBTestXactLogIterator : public DBTestBase {
 public:
  DBTestXactLogIterator()
      : DBTestBase("db_log_iter_test", /*env_do_fsync=*/true) {}

  std::unique_ptr<TransactionLogIterator> OpenTransactionLogIter(
      const SequenceNumber seq) {
    std::unique_ptr<TransactionLogIterator> iter;
    Status status = dbfull()->GetUpdatesSince(seq, &iter);
    EXPECT_OK(status);
    EXPECT_TRUE(iter->Valid());
    return iter;
  }
};

namespace {
SequenceNumber ReadRecords(std::unique_ptr<TransactionLogIterator>& iter,
                           int& count, bool expect_ok = true) {
  count = 0;
  SequenceNumber lastSequence = 0;
  BatchResult res;
  while (iter->Valid()) {
    res = iter->GetBatch();
    EXPECT_TRUE(res.sequence > lastSequence);
    ++count;
    lastSequence = res.sequence;
    EXPECT_OK(iter->status());
    iter->Next();
  }
  if (expect_ok) {
    EXPECT_OK(iter->status());
  } else {
    EXPECT_NOK(iter->status());
  }
  return res.sequence;
}

void ExpectRecords(
    const int expected_no_records,
    std::unique_ptr<TransactionLogIterator>& iter) {
  int num_records;
  ReadRecords(iter, num_records);
  ASSERT_EQ(num_records, expected_no_records);
}
}  // namespace

TEST_F(DBTestXactLogIterator, TransactionLogIterator) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(Put(0, "key1", DummyString(1024)));
    ASSERT_OK(Put(1, "key2", DummyString(1024)));
    ASSERT_OK(Put(1, "key2", DummyString(1024)));
    ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 3U);
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(3, iter);
    }
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    env_->SleepForMicroseconds(2 * 1000 * 1000);
    {
      ASSERT_OK(Put(0, "key4", DummyString(1024)));
      ASSERT_OK(Put(1, "key5", DummyString(1024)));
      ASSERT_OK(Put(0, "key6", DummyString(1024)));
    }
    {
      auto iter = OpenTransactionLogIter(0);
      ExpectRecords(6, iter);
    }
  } while (ChangeCompactOptions());
}

#ifndef NDEBUG  // sync point is not included with DNDEBUG build
TEST_F(DBTestXactLogIterator, TransactionLogIteratorRace) {
  static const int LOG_ITERATOR_RACE_TEST_COUNT = 2;
  static const char* sync_points[LOG_ITERATOR_RACE_TEST_COUNT][4] = {
      {"WalManager::GetSortedWalFiles:1",  "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2", "WalManager::GetSortedWalFiles:2"},
      {"WalManager::GetSortedWalsOfType:1",
       "WalManager::PurgeObsoleteFiles:1",
       "WalManager::PurgeObsoleteFiles:2",
       "WalManager::GetSortedWalsOfType:2"}};
  for (int test = 0; test < LOG_ITERATOR_RACE_TEST_COUNT; ++test) {
    // Setup sync point dependency to reproduce the race condition of
    // a log file moved to archived dir, in the middle of GetSortedWalFiles
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {sync_points[test][0], sync_points[test][1]},
        {sync_points[test][2], sync_points[test][3]},
    });

    do {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
      Options options = OptionsForLogIterTest();
      DestroyAndReopen(options);
      ASSERT_OK(Put("key1", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key2", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key3", DummyString(1024)));
      ASSERT_OK(dbfull()->Flush(FlushOptions()));
      ASSERT_OK(Put("key4", DummyString(1024)));
      ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 4U);
      ASSERT_OK(dbfull()->FlushWAL(false));

      {
        auto iter = OpenTransactionLogIter(0);
        ExpectRecords(4, iter);
      }

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
      // trigger async flush, and log move. Well, log move will
      // wait until the GetSortedWalFiles:1 to reproduce the race
      // condition
      FlushOptions flush_options;
      flush_options.wait = false;
      ASSERT_OK(dbfull()->Flush(flush_options));

      // "key5" would be written in a new memtable and log
      ASSERT_OK(Put("key5", DummyString(1024)));
      ASSERT_OK(dbfull()->FlushWAL(false));
      {
        // this iter would miss "key4" if not fixed
        auto iter = OpenTransactionLogIter(0);
        ExpectRecords(5, iter);
      }
    } while (ChangeCompactOptions());
  }
}
#endif

TEST_F(DBTestXactLogIterator, TransactionLogIteratorStallAtLastRecord) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(1024)));
    auto iter = OpenTransactionLogIter(0);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_OK(Put("key2", DummyString(1024)));
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, ZeroRangeSmallRecordsResumeAfterPublication) {
  for (bool manual : {false, true}) {
    Options options = OptionsForLogIterTest();
    options.env = Env::Default();
    options.enable_zero_range_preallocation = true;
    options.manual_wal_flush = manual;
    options.write_buffer_size = 1024 * 1024;
    DestroyAndReopen(options);
    ASSERT_OK(Put("one", "small"));
    if (manual) {
      ASSERT_OK(db_->FlushWAL(false));
    }
    std::unique_ptr<LogFile> wal;
    ASSERT_OK(db_->GetCurrentWalFile(&wal));
    ASSERT_GT(wal->SizeFileBytes(), 0U);
    uint64_t physical_size;
    ASSERT_OK(
        options.env->GetFileSize(dbname_ + wal->PathName(), &physical_size));
    ASSERT_GT(physical_size, wal->SizeFileBytes());
    auto iter = OpenTransactionLogIter(0);
    ASSERT_EQ(1U, iter->GetBatch().sequence);
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    // Same preallocated interval, well below libc's usual read buffer size.
    ASSERT_OK(Put("two", "tiny"));
    if (manual) {
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      // The existing iterator reports a temporary sequence/readable-prefix
      // gap until manual FlushWAL publishes the pending complete record.
      ASSERT_TRUE(iter->status().IsTryAgain());
      ASSERT_OK(db_->FlushWAL(false));
    }
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(2U, iter->GetBatch().sequence);
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(Put("three", std::string(40000, 'x')));
    if (manual) {
      ASSERT_OK(db_->FlushWAL(false));
    }
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(3U, iter->GetBatch().sequence);
    ASSERT_OK(iter->status());
    LiveFilesStorageInfoOptions snapshot_options;
    snapshot_options.wal_size_for_flush = port::kMaxUint64;
    std::vector<LiveFileStorageInfo> files;
    ASSERT_OK(db_->GetLiveFilesStorageInfo(snapshot_options, &files));
    bool found_wal = false;
    for (const auto& info : files) {
      if (info.file_type == kWalFile) {
        found_wal = true;
        ASSERT_TRUE(info.trim_to_size);
        ASSERT_LT(info.size, physical_size);
      }
    }
    ASSERT_TRUE(found_wal);
    // Checkpoint must copy only R even though the source WAL still has a
    // larger reserved extent, including manual-flush configurations.
    const std::string checkpoint_path = dbname_ + "_zero_checkpoint";
    ASSERT_OK(DestroyDB(checkpoint_path, options));
    Checkpoint* raw_checkpoint = nullptr;
    ASSERT_OK(Checkpoint::Create(db_, &raw_checkpoint));
    std::unique_ptr<Checkpoint> checkpoint(raw_checkpoint);
    ASSERT_OK(checkpoint->CreateCheckpoint(checkpoint_path, port::kMaxUint64));
    ASSERT_OK(db_->GetCurrentWalFile(&wal));
    uint64_t copied_size = 0;
    ASSERT_OK(options.env->GetFileSize(checkpoint_path + wal->PathName(),
                                      &copied_size));
    ASSERT_EQ(wal->SizeFileBytes(), copied_size);
    Options restore_options(options);
    restore_options.wal_dir.clear();
    restore_options.enable_zero_range_preallocation = false;
    DB* raw_restore = nullptr;
    ASSERT_OK(DB::Open(restore_options, checkpoint_path, &raw_restore));
    std::unique_ptr<DB> restore(raw_restore);
    std::string value;
    ASSERT_OK(restore->Get(ReadOptions(), "one", &value));
    ASSERT_EQ("small", value);
    ASSERT_OK(restore->Get(ReadOptions(), "two", &value));
    ASSERT_EQ("tiny", value);
    ASSERT_OK(restore->Get(ReadOptions(), "three", &value));
    ASSERT_EQ(std::string(40000, 'x'), value);
    ASSERT_OK(restore->Close());
    restore.reset();
    ASSERT_OK(DestroyDB(checkpoint_path, restore_options));
  }
}

#if GTEST_HAS_DEATH_TEST && defined(OS_LINUX)
TEST_F(DBTestXactLogIterator, ZeroRangeCrashRecoversSyncedRecordWithOptionOff) {
  // Re-exec for the child so inherited background-pool mutexes cannot affect
  // the crash simulation. _exit deliberately bypasses every writer destructor.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  Options options = OptionsForLogIterTest();
  options.env = Env::Default();
  options.enable_zero_range_preallocation = true;
  options.write_buffer_size = 1024 * 1024;
  DestroyAndReopen(options);
  Close();
  // PerThreadDBPath includes the PID, so the re-executed child must inherit
  // the parent's explicit database path rather than use its own fixture path.
  if (getenv("ROCKSDB_ZERO_RANGE_CRASH_DB") == nullptr) {
    ASSERT_EQ(0, setenv("ROCKSDB_ZERO_RANGE_CRASH_DB", dbname_.c_str(), 1));
  }
  const std::string crash_dbname = getenv("ROCKSDB_ZERO_RANGE_CRASH_DB");
  ASSERT_EXIT(
      {
        DB* crash_db = nullptr;
        if (!DB::Open(options, crash_dbname, &crash_db).ok()) _exit(10);
        WriteOptions write_options;
        write_options.sync = true;
        if (!crash_db->Put(write_options, "synced-before-crash", "durable")
                 .ok()) {
          _exit(11);
        }
        std::unique_ptr<LogFile> wal;
        if (!crash_db->GetCurrentWalFile(&wal).ok()) _exit(12);
        uint64_t physical_size = 0;
        if (!options.env->GetFileSize(crash_dbname + wal->PathName(), &physical_size)
                 .ok()) {
          _exit(13);
        }
        if (physical_size <= wal->SizeFileBytes() ||
            wal->SizeFileBytes() == 0) {
          _exit(14);
        }
        _exit(0);
      },
      ::testing::ExitedWithCode(0), "");
  ASSERT_EQ(0, unsetenv("ROCKSDB_ZERO_RANGE_CRASH_DB"));
  options.enable_zero_range_preallocation = false;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  Reopen(options);
  ASSERT_EQ("durable", Get("synced-before-crash"));
}
#endif

TEST_F(DBTestXactLogIterator, TransactionLogIteratorCheckAfterRestart) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", DummyString(1024)));
    ASSERT_OK(Put("key2", DummyString(1023)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
    Reopen(options);
    auto iter = OpenTransactionLogIter(0);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorCorruptedLog) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);

    for (int i = 0; i < 1024; i++) {
      ASSERT_OK(Put("key" + ToString(i), DummyString(10)));
    }

    ASSERT_OK(Flush());
    ASSERT_OK(db_->FlushWAL(false));

    // Corrupt this log to create a gap
    ASSERT_OK(db_->DisableFileDeletions());

    VectorLogPtr wal_files;
    ASSERT_OK(db_->GetSortedWalFiles(wal_files));
    ASSERT_FALSE(wal_files.empty());

    const auto logfile_path = dbname_ + "/" + wal_files.front()->PathName();
    ASSERT_OK(test::TruncateFile(env_, logfile_path,
                                 wal_files.front()->SizeFileBytes() / 2));

    ASSERT_OK(db_->EnableFileDeletions());

    // Insert a new entry to a new log file
    ASSERT_OK(Put("key1025", DummyString(10)));
    ASSERT_OK(db_->FlushWAL(false));

    // Try to read from the beginning. Should stop before the gap and read less
    // than 1025 entries
    auto iter = OpenTransactionLogIter(0);
    int count = 0;
    SequenceNumber last_sequence_read = ReadRecords(iter, count, false);
    ASSERT_LT(last_sequence_read, 1025U);

    // Try to read past the gap, should be able to seek to key1025
    auto iter2 = OpenTransactionLogIter(last_sequence_read + 1);
    ExpectRecords(1, iter2);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorBatchOperations) {
  do {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[1], "key1", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[0], "key2", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[1], "key3", DummyString(1024)));
    ASSERT_OK(batch.Delete(handles_[0], "key2"));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Flush(0));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_OK(Put(1, "key4", DummyString(1024)));
    auto iter = OpenTransactionLogIter(3);
    ExpectRecords(2, iter);
  } while (ChangeCompactOptions());
}

TEST_F(DBTestXactLogIterator, TransactionLogIteratorBlobs) {
  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  {
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[1], "key1", DummyString(1024)));
    ASSERT_OK(batch.Put(handles_[0], "key2", DummyString(1024)));
    ASSERT_OK(batch.PutLogData(Slice("blob1")));
    ASSERT_OK(batch.Put(handles_[1], "key3", DummyString(1024)));
    ASSERT_OK(batch.PutLogData(Slice("blob2")));
    ASSERT_OK(batch.Delete(handles_[0], "key2"));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
  }

  auto res = OpenTransactionLogIter(0)->GetBatch();
  struct Handler : public WriteBatch::Handler {
    std::string seen;
    Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
      seen += "Put(" + ToString(cf) + ", " + key.ToString() + ", " +
              ToString(value.size()) + ")";
      return Status::OK();
    }
    Status MergeCF(uint32_t cf, const Slice& key, const Slice& value) override {
      seen += "Merge(" + ToString(cf) + ", " + key.ToString() + ", " +
              ToString(value.size()) + ")";
      return Status::OK();
    }
    void LogData(const Slice& blob) override {
      seen += "LogData(" + blob.ToString() + ")";
    }
    Status DeleteCF(uint32_t cf, const Slice& key) override {
      seen += "Delete(" + ToString(cf) + ", " + key.ToString() + ")";
      return Status::OK();
    }
  } handler;
  ASSERT_OK(res.writeBatchPtr->Iterate(&handler));
  ASSERT_EQ(
      "Put(1, key1, 1024)"
      "Put(0, key2, 1024)"
      "LogData(blob1)"
      "Put(1, key3, 1024)"
      "LogData(blob2)"
      "Delete(0, key2)",
      handler.seen);
}
}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE)

int main(int argc, char** argv) {
#if !defined(ROCKSDB_LITE)
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
#else
  (void) argc;
  (void) argv;
  return 0;
#endif
}
