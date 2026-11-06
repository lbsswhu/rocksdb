// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <cstdint>
#if defined(OS_LINUX)
#include <climits>
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#else
#include <condition_variable>
#include <mutex>
#endif
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {
// Register, snapshot epoch, recheck work, then wait only for that epoch. SC
// registration/epoch operations ensure a notifier either observes a sleeper or
// precedes its epoch snapshot (whose acquire makes the published work visible).
class AsyncCommitEvent {
 public:
  uint32_t PrepareWait() {
    sleepers_.fetch_add(1);
    return epoch_.load();
  }
  void CancelWait() { sleepers_.fetch_sub(1); }
  void Wait(uint32_t observed) {
#if defined(OS_LINUX)
    static_assert(sizeof(epoch_) == sizeof(uint32_t), "futex word size");
    syscall(SYS_futex, reinterpret_cast<uint32_t*>(&epoch_),
            FUTEX_WAIT_PRIVATE, observed, nullptr, nullptr, 0);
#else
    // Only the idle event uses this portable sleeping mutex. Queue publication
    // and consumption remain lock-free; Linux notification needs no mutex.
    std::unique_lock<std::mutex> lock(sleep_mutex_);
    sleep_cv_.wait(lock, [&] { return epoch_.load() != observed; });
#endif
  }
  bool NotifyAll() {
    epoch_.fetch_add(1);
    if (sleepers_.load() == 0) return false;
#if defined(OS_LINUX)
    syscall(SYS_futex, reinterpret_cast<uint32_t*>(&epoch_),
            FUTEX_WAKE_PRIVATE, INT_MAX, nullptr, nullptr, 0);
#else
    std::lock_guard<std::mutex> lock(sleep_mutex_);
    sleep_cv_.notify_all();
#endif
    return true;
  }
 private:
  alignas(4) std::atomic<uint32_t> epoch_{0};
  std::atomic<uint32_t> sleepers_{0};
#if !defined(OS_LINUX)
  std::mutex sleep_mutex_;
  std::condition_variable sleep_cv_;
#endif
};
}  // namespace ROCKSDB_NAMESPACE
