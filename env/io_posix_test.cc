// Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "test_util/testharness.h"

#ifdef ROCKSDB_LIB_IO_POSIX
#include <fcntl.h>
#include <unistd.h>

#include "env/io_posix.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

TEST(PosixSequentialBufferingTest, FailureClosesCandidate) {
  char path[] = "/tmp/rocksdb-buffering-XXXXXX";
  int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(0, close(fd));
  auto fs = Env::Default()->GetFileSystem();
  FileOptions options;
  options.disable_stdio_buffering = true;
  int candidate_fd = -1;
  SyncPoint::GetInstance()->SetCallBack(
      "NewSequentialFile:Setvbuf", [&](void* arg) {
        auto* result = static_cast<std::pair<int, int>*>(arg);
        result->first = -1;
        candidate_fd = result->second;
        errno = 0;  // setvbuf does not promise to populate errno.
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<FSSequentialFile> file;
  IOStatus status = fs->NewSequentialFile(path, options, &file, nullptr);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_TRUE(status.IsIOError());
  EXPECT_EQ(nullptr, file.get());
  ASSERT_GE(candidate_fd, 0);
  EXPECT_EQ(-1, fcntl(candidate_fd, F_GETFD));
  EXPECT_EQ(EBADF, errno);
  ASSERT_EQ(0, unlink(path));
}

#if defined(ROCKSDB_FALLOCATE_PRESENT) && defined(FALLOC_FL_ZERO_RANGE)
class ZeroRangeAllocationTest : public testing::Test {
 protected:
  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
};

TEST_F(ZeroRangeAllocationTest, FallbackAndErrors) {
  for (int injected : {EOPNOTSUPP, ENOSYS, EINVAL, ENOSPC, EIO}) {
    char path[] = "/tmp/rocksdb-zero-range-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    EnvOptions options;
    options.use_mmap_writes = false;
    options.preallocation_mode = PreallocationMode::kZeroRange;
    PosixWritableFile file(path, fd, 4096, options);
    int attempts = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "PosixWritableFile::Allocate:ZeroRangeError", [&](void* arg) {
          ++attempts;
          *static_cast<int*>(arg) = injected;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    IOStatus status = file.Allocate(0, 4096, IOOptions(), nullptr);
    if (injected == EOPNOTSUPP || injected == ENOSYS) {
      ASSERT_OK(status);
      EXPECT_EQ(PreallocationMode::kLegacy, file.GetPreallocationMode());
      ASSERT_OK(file.Allocate(4096, 4096, IOOptions(), nullptr));
      EXPECT_EQ(1, attempts);
    } else {
      EXPECT_FALSE(status.ok());
      EXPECT_EQ(PreallocationMode::kZeroRange, file.GetPreallocationMode());
    }
    ASSERT_OK(file.Close(IOOptions(), nullptr));
    struct stat stats;
    ASSERT_EQ(0, stat(path, &stats));
    EXPECT_EQ(0, stats.st_size);
    ASSERT_EQ(0, unlink(path));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
}

TEST_F(ZeroRangeAllocationTest, InterruptedAllocationRetries) {
  char path[] = "/tmp/rocksdb-zero-range-XXXXXX";
  int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  EnvOptions options;
  options.use_mmap_writes = false;
  options.preallocation_mode = PreallocationMode::kZeroRange;
  PosixWritableFile file(path, fd, 4096, options);
  int attempts = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "PosixWritableFile::Allocate:ZeroRangeError", [&](void* arg) {
        *static_cast<int*>(arg) = attempts++ == 0 ? EINTR : ENOSYS;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(file.Allocate(0, 4096, IOOptions(), nullptr));
  EXPECT_EQ(2, attempts);
  ASSERT_OK(file.Close(IOOptions(), nullptr));
  ASSERT_EQ(0, unlink(path));
}

TEST_F(ZeroRangeAllocationTest, TruncateFailureClosesDescriptor) {
  char path[] = "/tmp/rocksdb-zero-range-XXXXXX";
  int fd = mkstemp(path);
  ASSERT_GE(fd, 0);
  EnvOptions options;
  options.use_mmap_writes = false;
  options.preallocation_mode = PreallocationMode::kZeroRange;
  PosixWritableFile file(path, fd, 4096, options);
  SyncPoint::GetInstance()->SetCallBack(
      "PosixWritableFile::Allocate:ZeroRangeError",
      [](void* arg) { *static_cast<int*>(arg) = ENOSYS; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(file.Allocate(0, 4096, IOOptions(), nullptr));
  SyncPoint::GetInstance()->SetCallBack(
      "PosixWritableFile::Truncate:Error",
      [](void* arg) { *static_cast<int*>(arg) = EIO; });
  EXPECT_TRUE(file.Close(IOOptions(), nullptr).IsIOError());
  EXPECT_EQ(-1, fcntl(fd, F_GETFD));
  EXPECT_EQ(EBADF, errno);
  ASSERT_EQ(0, unlink(path));
}
#endif

#ifdef OS_LINUX
class LogicalBlockSizeCacheTest : public testing::Test {};

// Tests the caching behavior.
TEST_F(LogicalBlockSizeCacheTest, Cache) {
  int ncall = 0;
  auto get_fd_block_size = [&](int fd) {
    ncall++;
    return fd;
  };
  std::map<std::string, int> dir_fds{
      {"/", 0},
      {"/db", 1},
      {"/db1", 2},
      {"/db2", 3},
  };
  auto get_dir_block_size = [&](const std::string& dir, size_t* size) {
    ncall++;
    *size = dir_fds[dir];
    return Status::OK();
  };
  LogicalBlockSizeCache cache(get_fd_block_size, get_dir_block_size);
  ASSERT_EQ(0, ncall);
  ASSERT_EQ(0, cache.Size());

  ASSERT_EQ(6, cache.GetLogicalBlockSize("/sst", 6));
  ASSERT_EQ(1, ncall);
  ASSERT_EQ(7, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(2, ncall);
  ASSERT_EQ(8, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(3, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/", "/db1/", "/db2"}));
  ASSERT_EQ(3, cache.Size());
  ASSERT_TRUE(cache.Contains("/"));
  ASSERT_TRUE(cache.Contains("/db1"));
  ASSERT_TRUE(cache.Contains("/db2"));
  ASSERT_EQ(6, ncall);
  // Block size for / is cached.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/sst", 6));
  ASSERT_EQ(6, ncall);
  // No cached size for /db.
  ASSERT_EQ(7, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(7, ncall);
  ASSERT_EQ(8, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(8, ncall);
  // Block size for /db1 is cached.
  ASSERT_EQ(2, cache.GetLogicalBlockSize("/db1/sst1", 4));
  ASSERT_EQ(8, ncall);
  ASSERT_EQ(2, cache.GetLogicalBlockSize("/db1/sst2", 5));
  ASSERT_EQ(8, ncall);
  // Block size for /db2 is cached.
  ASSERT_EQ(3, cache.GetLogicalBlockSize("/db2/sst1", 6));
  ASSERT_EQ(8, ncall);
  ASSERT_EQ(3, cache.GetLogicalBlockSize("/db2/sst2", 7));
  ASSERT_EQ(8, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(4, cache.Size());
  ASSERT_TRUE(cache.Contains("/"));
  ASSERT_TRUE(cache.Contains("/db1"));
  ASSERT_TRUE(cache.Contains("/db2"));
  ASSERT_TRUE(cache.Contains("/db"));

  ASSERT_EQ(9, ncall);
  // Block size for /db is cached.
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(9, ncall);
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(9, ncall);
}

// Tests the reference counting behavior.
TEST_F(LogicalBlockSizeCacheTest, Ref) {
  int ncall = 0;
  auto get_fd_block_size = [&](int fd) {
    ncall++;
    return fd;
  };
  std::map<std::string, int> dir_fds{
      {"/db", 0},
  };
  auto get_dir_block_size = [&](const std::string& dir, size_t* size) {
    ncall++;
    *size = dir_fds[dir];
    return Status::OK();
  };
  LogicalBlockSizeCache cache(get_fd_block_size, get_dir_block_size);

  ASSERT_EQ(0, ncall);

  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst0", 1));
  ASSERT_EQ(1, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(2, ncall);
  ASSERT_EQ(1, cache.GetRefCount("/db"));
  // Block size for /db is cached. Ref count = 1.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/db/sst1", 1));
  ASSERT_EQ(2, ncall);

  // Ref count = 2, but won't recompute the cached buffer size.
  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(2, cache.GetRefCount("/db"));
  ASSERT_EQ(2, ncall);

  // Ref count = 1.
  cache.UnrefAndTryRemoveCachedLogicalBlockSize({"/db"});
  ASSERT_EQ(1, cache.GetRefCount("/db"));
  // Block size for /db is still cached.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/db/sst2", 1));
  ASSERT_EQ(2, ncall);

  // Ref count = 0 and cached buffer size for /db is removed.
  cache.UnrefAndTryRemoveCachedLogicalBlockSize({"/db"});
  ASSERT_EQ(0, cache.Size());
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst0", 1));
  ASSERT_EQ(3, ncall);
}
#endif

}  // namespace ROCKSDB_NAMESPACE
#endif

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
