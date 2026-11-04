//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/writable_file_writer.h"

#include <algorithm>
#include <limits>
#include <mutex>

#include "db/version_edit.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
IOStatus WritableFileWriter::Create(const std::shared_ptr<FileSystem>& fs,
                                    const std::string& fname,
                                    const FileOptions& file_opts,
                                    std::unique_ptr<WritableFileWriter>* writer,
                                    IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> file;
  IOStatus io_s = fs->NewWritableFile(fname, file_opts, &file, dbg);
  if (io_s.ok()) {
    writer->reset(new WritableFileWriter(std::move(file), fname, file_opts));
  }
  return io_s;
}

IOStatus WritableFileWriter::RememberError(const IOStatus& status) {
  if (!status.ok() && first_error_.ok()) {
    first_error_ = status;
  }
  return status;
}

IOStatus WritableFileWriter::PrepareAppend(uint64_t offset, size_t len) {
  if (!zero_range_preallocation_) {
    writable_file_->PrepareWrite(static_cast<size_t>(offset), len, IOOptions(),
                                 nullptr);
    return IOStatus::OK();
  }
  if (len == 0) return IOStatus::OK();
  // A successful aligned reservation also covers the Direct I/O tail page.
  // Subtract before comparing so even oversized appends cannot wrap the end.
  if (offset <= reserved_end_ && len <= reserved_end_ - offset) {
    return IOStatus::OK();
  }
  const uint64_t limit =
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
  auto align_up = [limit](uint64_t value, uint64_t unit,
                          uint64_t* result) -> bool {
    if (unit == 0 || value > limit) return false;
    uint64_t extra = value % unit == 0 ? 0 : unit - value % unit;
    if (extra > limit - value) return false;
    *result = value + extra;
    return true;
  };
  if (preallocation_granularity_ == 0) {
    size_t block_size = 0, ignored = 0;
    writable_file_->GetPreallocationStatus(&block_size, &ignored);
    if (block_size == 0 ||
        !align_up(block_size, buf_.Alignment(), &preallocation_granularity_)) {
      return IOStatus::InvalidArgument(
          "Preallocation range overflow or zero granularity");
    }
  }
  uint64_t end = 0;
  if (offset > limit || len > limit - offset ||
      !align_up(offset + len, preallocation_granularity_, &end)) {
    return IOStatus::InvalidArgument(
        "Preallocation range overflow or zero granularity");
  }
  // Every Direct I/O write (including padding) is covered by a prior aligned
  // reservation. Never restart allocation at a logical offset within that page.
  needs_final_truncate_ = true;
  const auto previous_mode = writable_file_->GetPreallocationMode();
  const bool zero_attempt = previous_mode == PreallocationMode::kZeroRange;
  if (zero_attempt) RecordTick(stats_, ZERO_RANGE_PREALLOCATION_CALLS);
  StopWatch timer(clock_ ? clock_ : SystemClock::Default().get(),
                  zero_attempt ? stats_ : nullptr,
                  ZERO_RANGE_PREALLOCATION_MICROS);
  IOStatus status = writable_file_->Allocate(reserved_end_, end - reserved_end_,
                                             IOOptions(), nullptr);
  if (previous_mode == PreallocationMode::kZeroRange &&
      writable_file_->GetPreallocationMode() == PreallocationMode::kLegacy) {
    RecordTick(stats_, ZERO_RANGE_PREALLOCATION_FALLBACKS);
  }
  if (status.ok()) {
    if (writable_file_->GetPreallocationMode() ==
        PreallocationMode::kZeroRange) {
      RecordTick(stats_, ZERO_RANGE_PREALLOCATION_BYTES, end - reserved_end_);
      uint32_t calls = ZERO_RANGE_PREALLOCATION_OTHER_SST_CALLS;
      uint32_t bytes = ZERO_RANGE_PREALLOCATION_OTHER_SST_BYTES;
      switch (preallocation_source_) {
        case PreallocationSource::kWal:
          calls = ZERO_RANGE_PREALLOCATION_WAL_CALLS;
          bytes = ZERO_RANGE_PREALLOCATION_WAL_BYTES;
          break;
        case PreallocationSource::kFlush:
          calls = ZERO_RANGE_PREALLOCATION_FLUSH_CALLS;
          bytes = ZERO_RANGE_PREALLOCATION_FLUSH_BYTES;
          break;
        case PreallocationSource::kCompaction:
          calls = ZERO_RANGE_PREALLOCATION_COMPACTION_CALLS;
          bytes = ZERO_RANGE_PREALLOCATION_COMPACTION_BYTES;
          break;
        case PreallocationSource::kOtherSst:
          break;
      }
      RecordTick(stats_, calls);
      RecordTick(stats_, bytes, end - reserved_end_);
    }
    reserved_end_ = end;
  } else {
    if (zero_attempt) RecordTick(stats_, ZERO_RANGE_PREALLOCATION_FAILURES);
  }
  return status;
}

IOStatus WritableFileWriter::Append(const Slice& data,
                                    uint32_t crc32c_checksum) {
  const char* src = data.data();
  size_t left = data.size();
  if (!first_error_.ok()) return first_error_;
  if (sealing_ || sealed_ || closed_) {
    return IOStatus::InvalidArgument("Append after file seal/close");
  }
  IOStatus s;
  {
    IOSTATS_TIMER_GUARD(prepare_write_nanos);
    TEST_SYNC_POINT("WritableFileWriter::Append:BeforePrepareWrite");
    s = PrepareAppend(GetFileSize(), left);
  }
  if (!s.ok()) return RememberError(s);
  pending_sync_ = true;
  TEST_KILL_RANDOM_WITH_WEIGHT("WritableFileWriter::Append:0", REDUCE_ODDS2);
  UpdateFileChecksum(data);

  // See whether we need to enlarge the buffer to avoid the flush
  if (buf_.Capacity() - buf_.CurrentSize() < left) {
    for (size_t cap = buf_.Capacity();
         cap < max_buffer_size_;  // There is still room to increase
         cap *= 2) {
      // See whether the next available size is large enough.
      // Buffer will never be increased to more than max_buffer_size_.
      size_t desired_capacity = std::min(cap * 2, max_buffer_size_);
      if (desired_capacity - buf_.CurrentSize() >= left ||
          (use_direct_io() && desired_capacity == max_buffer_size_)) {
        buf_.AllocateNewBuffer(desired_capacity, true);
        break;
      }
    }
  }

  // Flush only when buffered I/O
  if (!use_direct_io() && (buf_.Capacity() - buf_.CurrentSize()) < left) {
    if (buf_.CurrentSize() > 0) {
      s = Flush();
      if (!s.ok()) {
        return s;
      }
    }
    assert(buf_.CurrentSize() == 0);
  }

  if (perform_data_verification_ && buffered_data_with_checksum_ &&
      crc32c_checksum != 0) {
    // Since we want to use the checksum of the input data, we cannot break it
    // into several pieces. We will only write them in the buffer when buffer
    // size is enough. Otherwise, we will directly write it down.
    if (use_direct_io() || (buf_.Capacity() - buf_.CurrentSize()) >= left) {
      if ((buf_.Capacity() - buf_.CurrentSize()) >= left) {
        size_t appended = buf_.Append(src, left);
        if (appended != left) {
          s = IOStatus::Corruption("Write buffer append failure");
        }
        buffered_data_crc32c_checksum_ = crc32c::Crc32cCombine(
            buffered_data_crc32c_checksum_, crc32c_checksum, appended);
      } else {
        while (left > 0) {
          size_t appended = buf_.Append(src, left);
          buffered_data_crc32c_checksum_ =
              crc32c::Extend(buffered_data_crc32c_checksum_, src, appended);
          left -= appended;
          src += appended;

          if (left > 0) {
            s = Flush();
            if (!s.ok()) {
              break;
            }
          }
        }
      }
    } else {
      assert(buf_.CurrentSize() == 0);
      buffered_data_crc32c_checksum_ = crc32c_checksum;
      s = WriteBufferedWithChecksum(src, left);
    }
  } else {
    // In this case, either we do not need to do the data verification or
    // caller does not provide the checksum of the data (crc32c_checksum = 0).
    //
    // We never write directly to disk with direct I/O on.
    // or we simply use it for its original purpose to accumulate many small
    // chunks
    if (use_direct_io() || (buf_.Capacity() >= left)) {
      while (left > 0) {
        size_t appended = buf_.Append(src, left);
        if (perform_data_verification_ && buffered_data_with_checksum_) {
          buffered_data_crc32c_checksum_ =
              crc32c::Extend(buffered_data_crc32c_checksum_, src, appended);
        }
        left -= appended;
        src += appended;

        if (left > 0) {
          s = Flush();
          if (!s.ok()) {
            break;
          }
        }
      }
    } else {
      // Writing directly to file bypassing the buffer
      assert(buf_.CurrentSize() == 0);
      if (perform_data_verification_ && buffered_data_with_checksum_) {
        buffered_data_crc32c_checksum_ = crc32c::Value(src, left);
        s = WriteBufferedWithChecksum(src, left);
      } else {
        s = WriteBuffered(src, left);
      }
    }
  }

  TEST_KILL_RANDOM("WritableFileWriter::Append:1");
  if (s.ok()) {
    filesize_ += data.size();
  }
  return RememberError(s);
}

IOStatus WritableFileWriter::Pad(const size_t pad_bytes) {
  if (!first_error_.ok()) return first_error_;
  if (sealing_ || sealed_ || closed_) {
    return IOStatus::InvalidArgument("Pad after file seal/close");
  }
  IOStatus prepare = PrepareAppend(filesize_, pad_bytes);
  if (!prepare.ok()) return RememberError(prepare);
  assert(pad_bytes < kDefaultPageSize);
  size_t left = pad_bytes;
  size_t cap = buf_.Capacity() - buf_.CurrentSize();
  size_t pad_start = buf_.CurrentSize();

  // Assume pad_bytes is small compared to buf_ capacity. So we always
  // use buf_ rather than write directly to file in certain cases like
  // Append() does.
  while (left) {
    size_t append_bytes = std::min(cap, left);
    buf_.PadWith(append_bytes, 0);
    left -= append_bytes;
    if (left > 0) {
      IOStatus s = Flush();
      if (!s.ok()) {
        return s;
      }
    }
    cap = buf_.Capacity() - buf_.CurrentSize();
  }
  pending_sync_ = true;
  filesize_ += pad_bytes;
  if (perform_data_verification_) {
    buffered_data_crc32c_checksum_ =
        crc32c::Extend(buffered_data_crc32c_checksum_,
                       buf_.BufferStart() + pad_start, pad_bytes);
  }
  return IOStatus::OK();
}

IOStatus WritableFileWriter::Seal() {
  if (!first_error_.ok()) return first_error_;
  if (sealed_) return IOStatus::OK();
  if (closed_ || sealing_) return IOStatus::InvalidArgument("File is not open");
  sealing_ = true;
  IOStatus status = Flush();
  if (!status.ok()) return RememberError(status);
  if (zero_range_preallocation_ || needs_final_truncate_ || use_direct_io()) {
    RecordTick(stats_, PREALLOCATION_TRUNCATE_CALLS);
    StopWatch timer(clock_ ? clock_ : SystemClock::Default().get(), stats_,
                    PREALLOCATION_TRUNCATE_MICROS);
#ifndef ROCKSDB_LITE
    auto start = FileOperationInfo::StartNow();
#endif
    status = writable_file_->Truncate(filesize_, IOOptions(), nullptr);
#ifndef ROCKSDB_LITE
    if (ShouldNotifyListeners()) {
      NotifyOnFileTruncateFinish(start, FileOperationInfo::FinishNow(), status);
    }
#endif
    if (!status.ok()) {
      RecordTick(stats_, PREALLOCATION_TRUNCATE_FAILURES);
      return RememberError(status);
    }
    needs_final_truncate_ = false;
    final_truncate_pending_sync_ = true;
    pending_sync_ = true;
  }
  sealed_ = true;
  // Direct I/O Flush retains a tail for subsequent appends. It must never be
  // rewritten after the final truncate.
  buf_.Size(0);
  return IOStatus::OK();
}

IOStatus WritableFileWriter::FinishAndClose(bool use_fsync) {
  if (closed_) return close_status_;
  IOStatus status = Seal();
  if (status.ok()) {
    // A final size change requires a real sync, including Direct I/O files.
    const bool covered = use_fsync
                             ? sealed_fsynced_.load(std::memory_order_acquire)
                             : sealed_synced_.load(std::memory_order_acquire);
    if (!covered) status = SyncInternal(use_fsync);
    if (status.ok()) {
      pending_sync_ = false;
      final_truncate_pending_sync_ = false;
    }
    RememberError(status).PermitUncheckedError();
  }
  return Close();
}

IOStatus WritableFileWriter::Close() {
  if (closed_) return close_status_;
  if (writable_file_.get() == nullptr) return IOStatus::OK();
  if (first_error_.ok() && !sealed_) {
    RememberError(Seal()).PermitUncheckedError();
  }
  // Preserve historical Direct I/O Close durability, but never rewrite a tail.
  if (first_error_.ok() && final_truncate_pending_sync_ && use_direct_io()) {
    IOStatus sync = SyncInternal(true);
    if (sync.ok()) final_truncate_pending_sync_ = false;
    RememberError(sync).PermitUncheckedError();
  }
  TEST_KILL_RANDOM("WritableFileWriter::Close:0");
#ifndef ROCKSDB_LITE
  auto start = FileOperationInfo::StartNow();
#endif
  IOStatus status = writable_file_->Close(IOOptions(), nullptr);
#ifndef ROCKSDB_LITE
  if (ShouldNotifyListeners()) {
    NotifyOnFileCloseFinish(start, FileOperationInfo::FinishNow(), status);
  }
#endif
  RememberError(status).PermitUncheckedError();
  writable_file_.reset();
  closed_ = true;
  close_status_ = first_error_;
  TEST_KILL_RANDOM("WritableFileWriter::Close:1");
  if (close_status_.ok() && checksum_generator_ != nullptr &&
      !checksum_finalized_) {
    checksum_generator_->Finalize();
    checksum_finalized_ = true;
  }
  return close_status_;
}

// write out the cached data to the OS cache or storage if direct I/O
// enabled
IOStatus WritableFileWriter::Flush() {
  if (!first_error_.ok()) return first_error_;
  if (sealed_) return IOStatus::OK();
  if (closed_) return close_status_;
  IOStatus s;
  TEST_KILL_RANDOM_WITH_WEIGHT("WritableFileWriter::Flush:0", REDUCE_ODDS2);

  if (buf_.CurrentSize() > 0) {
    if (use_direct_io()) {
#ifndef ROCKSDB_LITE
      if (pending_sync_) {
        if (perform_data_verification_ && buffered_data_with_checksum_) {
          s = WriteDirectWithChecksum();
        } else {
          s = WriteDirect();
        }
      }
#endif  // !ROCKSDB_LITE
    } else {
      if (perform_data_verification_ && buffered_data_with_checksum_) {
        s = WriteBufferedWithChecksum(buf_.BufferStart(), buf_.CurrentSize());
      } else {
        s = WriteBuffered(buf_.BufferStart(), buf_.CurrentSize());
      }
    }
    if (!s.ok()) {
      return RememberError(s);
    }
  }

  {
#ifndef ROCKSDB_LITE
    FileOperationInfo::StartTimePoint start_ts;
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
    }
#endif
    s = writable_file_->Flush(IOOptions(), nullptr);
#ifndef ROCKSDB_LITE
    if (ShouldNotifyListeners()) {
      auto finish_ts = std::chrono::steady_clock::now();
      NotifyOnFileFlushFinish(start_ts, finish_ts, s);
    }
#endif
  }

  if (!s.ok()) {
    return RememberError(s);
  }

  // sync OS cache to disk for every bytes_per_sync_
  // TODO: give log file and sst file different options (log
  // files could be potentially cached in OS for their whole
  // life time, thus we might not want to flush at all).

  // We try to avoid sync to the last 1MB of data. For two reasons:
  // (1) avoid rewrite the same page that is modified later.
  // (2) for older version of OS, write can block while writing out
  //     the page.
  // Xfs does neighbor page flushing outside of the specified ranges. We
  // need to make sure sync range is far from the write offset.
  if (!use_direct_io() && bytes_per_sync_) {
    const uint64_t kBytesNotSyncRange =
        1024 * 1024;                                // recent 1MB is not synced.
    const uint64_t kBytesAlignWhenSync = 4 * 1024;  // Align 4KB.
    if (filesize_ > kBytesNotSyncRange) {
      uint64_t offset_sync_to = filesize_ - kBytesNotSyncRange;
      offset_sync_to -= offset_sync_to % kBytesAlignWhenSync;
      assert(offset_sync_to >= last_sync_size_);
      if (offset_sync_to > 0 &&
          offset_sync_to - last_sync_size_ >= bytes_per_sync_) {
        s = RangeSync(last_sync_size_, offset_sync_to - last_sync_size_);
        last_sync_size_ = offset_sync_to;
      }
    }
  }

  return RememberError(s);
}

std::string WritableFileWriter::GetFileChecksum() {
  if (checksum_generator_ != nullptr) {
    assert(checksum_finalized_);
    return checksum_generator_->GetChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* WritableFileWriter::GetFileChecksumFuncName() const {
  if (checksum_generator_ != nullptr) {
    return checksum_generator_->Name();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}

IOStatus WritableFileWriter::Sync(bool use_fsync) {
  if (closed_) return close_status_;
  IOStatus s = Flush();
  if (!s.ok()) {
    return s;
  }
  TEST_KILL_RANDOM("WritableFileWriter::Sync:0");
  if (final_truncate_pending_sync_ || (!use_direct_io() && pending_sync_)) {
    s = SyncInternal(use_fsync);
    if (!s.ok()) {
      return RememberError(s);
    }
  }
  TEST_KILL_RANDOM("WritableFileWriter::Sync:1");
  pending_sync_ = false;
  final_truncate_pending_sync_ = false;
  return IOStatus::OK();
}

IOStatus WritableFileWriter::SyncWithoutFlush(bool use_fsync) {
  if (!writable_file_->IsSyncThreadSafe()) {
    return IOStatus::NotSupported(
        "Can't WritableFileWriter::SyncWithoutFlush() because "
        "WritableFile::IsSyncThreadSafe() is false");
  }
  TEST_SYNC_POINT("WritableFileWriter::SyncWithoutFlush:1");
  IOStatus s = SyncInternal(use_fsync);
  TEST_SYNC_POINT("WritableFileWriter::SyncWithoutFlush:2");
  return s;
}

IOStatus WritableFileWriter::SyncInternal(bool use_fsync) {
  IOStatus s;
  IOSTATS_TIMER_GUARD(fsync_nanos);
  if (sealed_) RecordTick(stats_, PREALLOCATION_FINAL_SYNC_CALLS);
  StopWatch final_timer(clock_ ? clock_ : SystemClock::Default().get(),
                        sealed_ ? stats_ : nullptr,
                        PREALLOCATION_FINAL_SYNC_MICROS);
  TEST_SYNC_POINT("WritableFileWriter::SyncInternal:0");
  auto prev_perf_level = GetPerfLevel();
  IOSTATS_CPU_TIMER_GUARD(cpu_write_nanos, clock_);
#ifndef ROCKSDB_LITE
  FileOperationInfo::StartTimePoint start_ts;
  if (ShouldNotifyListeners()) {
    start_ts = FileOperationInfo::StartNow();
  }
#endif
  if (use_fsync) {
    s = writable_file_->Fsync(IOOptions(), nullptr);
  } else {
    s = writable_file_->Sync(IOOptions(), nullptr);
  }
#ifndef ROCKSDB_LITE
  if (ShouldNotifyListeners()) {
    auto finish_ts = std::chrono::steady_clock::now();
    NotifyOnFileSyncFinish(
        start_ts, finish_ts, s,
        use_fsync ? FileOperationType::kFsync : FileOperationType::kSync);
  }
#endif
  SetPerfLevel(prev_perf_level);
  if (s.ok() && sealed_) {
    if (use_fsync) sealed_fsynced_.store(true, std::memory_order_release);
    sealed_synced_.store(true, std::memory_order_release);
  }
  return s;
}

IOStatus WritableFileWriter::RangeSync(uint64_t offset, uint64_t nbytes) {
  IOSTATS_TIMER_GUARD(range_sync_nanos);
  TEST_SYNC_POINT("WritableFileWriter::RangeSync:0");
#ifndef ROCKSDB_LITE
  FileOperationInfo::StartTimePoint start_ts;
  if (ShouldNotifyListeners()) {
    start_ts = FileOperationInfo::StartNow();
  }
#endif
  IOStatus s = writable_file_->RangeSync(offset, nbytes, IOOptions(), nullptr);
#ifndef ROCKSDB_LITE
  if (ShouldNotifyListeners()) {
    auto finish_ts = std::chrono::steady_clock::now();
    NotifyOnFileRangeSyncFinish(offset, nbytes, start_ts, finish_ts, s);
  }
#endif
  return s;
}

// This method writes to disk the specified data and makes use of the rate
// limiter if available
IOStatus WritableFileWriter::WriteBuffered(const char* data, size_t size) {
  IOStatus s;
  assert(!use_direct_io());
  const char* src = data;
  size_t left = size;
  DataVerificationInfo v_info;
  char checksum_buf[sizeof(uint32_t)];

  while (left > 0) {
    size_t allowed;
    if (rate_limiter_ != nullptr) {
      allowed = rate_limiter_->RequestToken(
          left, 0 /* alignment */, writable_file_->GetIOPriority(), stats_,
          RateLimiter::OpType::kWrite);
    } else {
      allowed = left;
    }

    {
      IOSTATS_TIMER_GUARD(write_nanos);
      TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");

#ifndef ROCKSDB_LITE
      FileOperationInfo::StartTimePoint start_ts;
      uint64_t old_size = writable_file_->GetFileSize(IOOptions(), nullptr);
      if (ShouldNotifyListeners()) {
        start_ts = FileOperationInfo::StartNow();
        old_size = next_write_offset_;
      }
#endif
      {
        auto prev_perf_level = GetPerfLevel();

        IOSTATS_CPU_TIMER_GUARD(cpu_write_nanos, clock_);
        if (perform_data_verification_) {
          Crc32cHandoffChecksumCalculation(src, allowed, checksum_buf);
          v_info.checksum = Slice(checksum_buf, sizeof(uint32_t));
          s = writable_file_->Append(Slice(src, allowed), IOOptions(), v_info,
                                     nullptr);
        } else {
          s = writable_file_->Append(Slice(src, allowed), IOOptions(), nullptr);
        }
        SetPerfLevel(prev_perf_level);
      }
#ifndef ROCKSDB_LITE
      if (ShouldNotifyListeners()) {
        auto finish_ts = std::chrono::steady_clock::now();
        NotifyOnFileWriteFinish(old_size, allowed, start_ts, finish_ts, s);
      }
#endif
      if (!s.ok()) {
        return s;
      }
    }

    IOSTATS_ADD(bytes_written, allowed);
    TEST_KILL_RANDOM("WritableFileWriter::WriteBuffered:0");

    left -= allowed;
    src += allowed;
  }
  buf_.Size(0);
  buffered_data_crc32c_checksum_ = 0;
  return s;
}

IOStatus WritableFileWriter::WriteBufferedWithChecksum(const char* data,
                                                       size_t size) {
  IOStatus s;
  assert(!use_direct_io());
  assert(perform_data_verification_ && buffered_data_with_checksum_);
  const char* src = data;
  size_t left = size;
  DataVerificationInfo v_info;
  char checksum_buf[sizeof(uint32_t)];

  // Check how much is allowed. Here, we loop until the rate limiter allows to
  // write the entire buffer.
  // TODO: need to be improved since it sort of defeats the purpose of the rate
  // limiter
  size_t data_size = left;
  if (rate_limiter_ != nullptr) {
    while (data_size > 0) {
      size_t tmp_size;
      tmp_size = rate_limiter_->RequestToken(
          data_size, buf_.Alignment(), writable_file_->GetIOPriority(), stats_,
          RateLimiter::OpType::kWrite);
      data_size -= tmp_size;
    }
  }

  {
    IOSTATS_TIMER_GUARD(write_nanos);
    TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");

#ifndef ROCKSDB_LITE
    FileOperationInfo::StartTimePoint start_ts;
    uint64_t old_size = writable_file_->GetFileSize(IOOptions(), nullptr);
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
      old_size = next_write_offset_;
    }
#endif
    {
      auto prev_perf_level = GetPerfLevel();

      IOSTATS_CPU_TIMER_GUARD(cpu_write_nanos, clock_);

      EncodeFixed32(checksum_buf, buffered_data_crc32c_checksum_);
      v_info.checksum = Slice(checksum_buf, sizeof(uint32_t));
      s = writable_file_->Append(Slice(src, left), IOOptions(), v_info,
                                 nullptr);
      SetPerfLevel(prev_perf_level);
    }
#ifndef ROCKSDB_LITE
    if (ShouldNotifyListeners()) {
      auto finish_ts = std::chrono::steady_clock::now();
      NotifyOnFileWriteFinish(old_size, left, start_ts, finish_ts, s);
    }
#endif
    if (!s.ok()) {
      return s;
    }
  }

  IOSTATS_ADD(bytes_written, left);
  TEST_KILL_RANDOM("WritableFileWriter::WriteBuffered:0");

  // Buffer write is successful, reset the buffer current size to 0 and reset
  // the corresponding checksum value
  buf_.Size(0);
  buffered_data_crc32c_checksum_ = 0;
  return s;
}

void WritableFileWriter::UpdateFileChecksum(const Slice& data) {
  if (checksum_generator_ != nullptr) {
    checksum_generator_->Update(data.data(), data.size());
  }
}

// Currently, crc32c checksum is used to calculate the checksum value of the
// content in the input buffer for handoff. In the future, the checksum might be
// calculated from the existing crc32c checksums of the in WAl and Manifest
// records, or even SST file blocks.
// TODO: effectively use the existing checksum of the data being writing to
// generate the crc32c checksum instead of a raw calculation.
void WritableFileWriter::Crc32cHandoffChecksumCalculation(const char* data,
                                                          size_t size,
                                                          char* buf) {
  uint32_t v_crc32c = crc32c::Extend(0, data, size);
  EncodeFixed32(buf, v_crc32c);
}

// This flushes the accumulated data in the buffer. We pad data with zeros if
// necessary to the whole page.
// However, during automatic flushes padding would not be necessary.
// We always use RateLimiter if available. We move (Refit) any buffer bytes
// that are left over the
// whole number of pages to be written again on the next flush because we can
// only write on aligned
// offsets.
#ifndef ROCKSDB_LITE
IOStatus WritableFileWriter::WriteDirect() {
  assert(use_direct_io());
  IOStatus s;
  const size_t alignment = buf_.Alignment();
  assert((next_write_offset_ % alignment) == 0);

  // Calculate whole page final file advance if all writes succeed
  size_t file_advance = TruncateToPageBoundary(alignment, buf_.CurrentSize());

  // Calculate the leftover tail, we write it here padded with zeros BUT we
  // will write
  // it again in the future either on Close() OR when the current whole page
  // fills out
  size_t leftover_tail = buf_.CurrentSize() - file_advance;

  // Round up and pad
  buf_.PadToAlignmentWith(0);

  const char* src = buf_.BufferStart();
  uint64_t write_offset = next_write_offset_;
  size_t left = buf_.CurrentSize();
  DataVerificationInfo v_info;
  char checksum_buf[sizeof(uint32_t)];

  while (left > 0) {
    // Check how much is allowed
    size_t size;
    if (rate_limiter_ != nullptr) {
      size = rate_limiter_->RequestToken(left, buf_.Alignment(),
                                         writable_file_->GetIOPriority(),
                                         stats_, RateLimiter::OpType::kWrite);
    } else {
      size = left;
    }

    {
      IOSTATS_TIMER_GUARD(write_nanos);
      TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");
      FileOperationInfo::StartTimePoint start_ts;
      if (ShouldNotifyListeners()) {
        start_ts = FileOperationInfo::StartNow();
      }
      // direct writes must be positional
      if (perform_data_verification_) {
        Crc32cHandoffChecksumCalculation(src, size, checksum_buf);
        v_info.checksum = Slice(checksum_buf, sizeof(uint32_t));
        s = writable_file_->PositionedAppend(Slice(src, size), write_offset,
                                             IOOptions(), v_info, nullptr);
      } else {
        s = writable_file_->PositionedAppend(Slice(src, size), write_offset,
                                             IOOptions(), nullptr);
      }

      if (ShouldNotifyListeners()) {
        auto finish_ts = std::chrono::steady_clock::now();
        NotifyOnFileWriteFinish(write_offset, size, start_ts, finish_ts, s);
      }
      if (!s.ok()) {
        buf_.Size(file_advance + leftover_tail);
        return s;
      }
    }

    IOSTATS_ADD(bytes_written, size);
    left -= size;
    src += size;
    write_offset += size;
    assert((next_write_offset_ % alignment) == 0);
  }

  if (s.ok()) {
    // Move the tail to the beginning of the buffer
    // This never happens during normal Append but rather during
    // explicit call to Flush()/Sync() or Close()
    buf_.RefitTail(file_advance, leftover_tail);
    // This is where we start writing next time which may or not be
    // the actual file size on disk. They match if the buffer size
    // is a multiple of whole pages otherwise filesize_ is leftover_tail
    // behind
    next_write_offset_ += file_advance;
  }
  return s;
}

IOStatus WritableFileWriter::WriteDirectWithChecksum() {
  assert(use_direct_io());
  assert(perform_data_verification_ && buffered_data_with_checksum_);
  IOStatus s;
  const size_t alignment = buf_.Alignment();
  assert((next_write_offset_ % alignment) == 0);

  // Calculate whole page final file advance if all writes succeed
  size_t file_advance = TruncateToPageBoundary(alignment, buf_.CurrentSize());

  // Calculate the leftover tail, we write it here padded with zeros BUT we
  // will write
  // it again in the future either on Close() OR when the current whole page
  // fills out
  size_t leftover_tail = buf_.CurrentSize() - file_advance;

  // Round up, pad, and combine the checksum.
  size_t last_cur_size = buf_.CurrentSize();
  buf_.PadToAlignmentWith(0);
  size_t padded_size = buf_.CurrentSize() - last_cur_size;
  const char* padded_start = buf_.BufferStart() + last_cur_size;
  uint32_t padded_checksum = crc32c::Value(padded_start, padded_size);
  buffered_data_crc32c_checksum_ = crc32c::Crc32cCombine(
      buffered_data_crc32c_checksum_, padded_checksum, padded_size);

  const char* src = buf_.BufferStart();
  uint64_t write_offset = next_write_offset_;
  size_t left = buf_.CurrentSize();
  DataVerificationInfo v_info;
  char checksum_buf[sizeof(uint32_t)];

  // Check how much is allowed. Here, we loop until the rate limiter allows to
  // write the entire buffer.
  // TODO: need to be improved since it sort of defeats the purpose of the rate
  // limiter
  size_t data_size = left;
  if (rate_limiter_ != nullptr) {
    while (data_size > 0) {
      size_t size;
      size = rate_limiter_->RequestToken(data_size, buf_.Alignment(),
                                         writable_file_->GetIOPriority(),
                                         stats_, RateLimiter::OpType::kWrite);
      data_size -= size;
    }
  }

  {
    IOSTATS_TIMER_GUARD(write_nanos);
    TEST_SYNC_POINT("WritableFileWriter::Flush:BeforeAppend");
    FileOperationInfo::StartTimePoint start_ts;
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
    }
    // direct writes must be positional
    EncodeFixed32(checksum_buf, buffered_data_crc32c_checksum_);
    v_info.checksum = Slice(checksum_buf, sizeof(uint32_t));
    s = writable_file_->PositionedAppend(Slice(src, left), write_offset,
                                         IOOptions(), v_info, nullptr);

    if (ShouldNotifyListeners()) {
      auto finish_ts = std::chrono::steady_clock::now();
      NotifyOnFileWriteFinish(write_offset, left, start_ts, finish_ts, s);
    }
    if (!s.ok()) {
      // In this case, we do not change buffered_data_crc32c_checksum_ because
      // it still aligns with the data in the buffer.
      buf_.Size(file_advance + leftover_tail);
      buffered_data_crc32c_checksum_ =
          crc32c::Value(buf_.BufferStart(), buf_.CurrentSize());
      return s;
    }
  }

  IOSTATS_ADD(bytes_written, left);
  assert((next_write_offset_ % alignment) == 0);

  if (s.ok()) {
    // Move the tail to the beginning of the buffer
    // This never happens during normal Append but rather during
    // explicit call to Flush()/Sync() or Close(). Also the buffer checksum will
    // recalculated accordingly.
    buf_.RefitTail(file_advance, leftover_tail);
    // Adjust the checksum value to align with the data in the buffer
    buffered_data_crc32c_checksum_ =
        crc32c::Value(buf_.BufferStart(), buf_.CurrentSize());
    // This is where we start writing next time which may or not be
    // the actual file size on disk. They match if the buffer size
    // is a multiple of whole pages otherwise filesize_ is leftover_tail
    // behind
    next_write_offset_ += file_advance;
  }
  return s;
}
#endif  // !ROCKSDB_LITE
}  // namespace ROCKSDB_NAMESPACE
