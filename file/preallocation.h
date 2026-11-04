// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// This source code is licensed under both the GPLv2 and Apache 2.0 License.
#pragma once

#include <algorithm>
#include <cstdint>
#include <limits>

#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

// Only target-file creation sites opt in. Ordinary EnvOptions remain legacy.
inline void ConfigureZeroRangePreallocation(const FileSystem* fs, bool enabled,
                                            FileOptions* options) {
  options->preallocation_mode = enabled && options->allow_fallocate &&
                                        !options->use_mmap_writes &&
                                        fs->SupportsZeroRangePreallocation()
                                    ? PreallocationMode::kZeroRange
                                    : PreallocationMode::kLegacy;
}

inline IOStatus SetSstPreallocationBlockSize(FSWritableFile* file,
                                             size_t write_buffer_size) {
  if (file->GetPreallocationMode() != PreallocationMode::kZeroRange) {
    return IOStatus::OK();
  }
  if (write_buffer_size == 0 ||
      write_buffer_size >
          std::numeric_limits<size_t>::max() - write_buffer_size / 10) {
    return IOStatus::InvalidArgument("Invalid SST preallocation size");
  }
  const size_t alignment = file->GetRequiredBufferAlignment();
  size_t block = std::min<size_t>(write_buffer_size + write_buffer_size / 10,
                                  size_t{1} << 30);
  if (alignment == 0 ||
      block > std::numeric_limits<size_t>::max() - (alignment - 1)) {
    return IOStatus::InvalidArgument("Invalid preallocation alignment");
  }
  block = ((block + alignment - 1) / alignment) * alignment;
  if (block == 0 || block > uint64_t{INT64_MAX}) {
    return IOStatus::InvalidArgument("SST preallocation exceeds file limit");
  }
  file->SetPreallocationBlockSize(block);
  return IOStatus::OK();
}

}  // namespace ROCKSDB_NAMESPACE
