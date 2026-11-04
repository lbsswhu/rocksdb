// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 and Apache 2.0 License.
#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

// Published only after a whole WAL record has reached the underlying file.
// Readers retain this object even after the writer or registry entry is gone.
struct WalReadBoundary {
  std::atomic<uint64_t> readable_end{0};
  std::atomic<bool> sealed{false};
};

class WalReadBoundaryRegistry {
 public:
  void Register(uint64_t number,
                const std::shared_ptr<WalReadBoundary>& boundary) {
    std::lock_guard<std::mutex> lock(mutex_);
    boundaries_[number] = boundary;
  }
  std::shared_ptr<WalReadBoundary> Get(uint64_t number) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = boundaries_.find(number);
    return it == boundaries_.end() ? nullptr : it->second;
  }
  void Unregister(uint64_t number) {
    std::lock_guard<std::mutex> lock(mutex_);
    boundaries_.erase(number);
  }

 private:
  mutable std::mutex mutex_;
  std::unordered_map<uint64_t, std::shared_ptr<WalReadBoundary>> boundaries_;
};

// Install BELOW any readahead. The underlying file must also have all hidden
// userspace buffering disabled before its first read (FileOptions).
class BoundedWalSequentialFile : public FSSequentialFileOwnerWrapper {
 public:
  BoundedWalSequentialFile(std::unique_ptr<FSSequentialFile>&& file,
                           std::shared_ptr<WalReadBoundary> boundary)
      : FSSequentialFileOwnerWrapper(std::move(file)),
        boundary_(std::move(boundary)),
        position_(0) {}

  IOStatus Read(size_t n, const IOOptions& opts, Slice* result, char* scratch,
                IODebugContext* dbg) override {
    const uint64_t limit =
        boundary_->readable_end.load(std::memory_order_acquire);
    n = static_cast<size_t>(
        std::min<uint64_t>(n, limit > position_ ? limit - position_ : 0));
    if (n == 0) {
      *result = Slice();
      return IOStatus::OK();
    }
    IOStatus s = target()->Read(n, opts, result, scratch, dbg);
    position_ += result->size();
    return s;
  }

  IOStatus Skip(uint64_t n) override {
    const uint64_t limit =
        boundary_->readable_end.load(std::memory_order_acquire);
    n = std::min(n, limit > position_ ? limit - position_ : 0);
    if (n == 0) return IOStatus::OK();
    IOStatus s = target()->Skip(n);
    if (s.ok()) position_ += n;
    return s;
  }

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& opts,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override {
    const uint64_t limit =
        boundary_->readable_end.load(std::memory_order_acquire);
    n = static_cast<size_t>(
        std::min<uint64_t>(n, limit > offset ? limit - offset : 0));
    if (n == 0) {
      *result = Slice();
      return IOStatus::OK();
    }
    return target()->PositionedRead(offset, n, opts, result, scratch, dbg);
  }

 private:
  std::shared_ptr<WalReadBoundary> boundary_;
  uint64_t position_;
};

}  // namespace ROCKSDB_NAMESPACE
