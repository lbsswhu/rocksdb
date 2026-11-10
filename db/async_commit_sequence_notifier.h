// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <thread>

#include "db/dbformat.h"
#include "util/async_commit_event.h"

namespace ROCKSDB_NAMESPACE {
class DBImpl;

// One independent thread per async DB. Workers publish the continuous visible
// prefix; this thread only schedules publication waiters, never owns groups or
// changes the read visibility boundary.
class AsyncCommitSequenceNotifier {
 public:
  explicit AsyncCommitSequenceNotifier(DBImpl* db) : db_(db) {}
  ~AsyncCommitSequenceNotifier();
  void Start(SequenceNumber initial_sequence);
  void Stop();
  void PublicationAdvanced();
  void RegisterWaiter();
  void UnregisterWaiter();

 private:
  void Run(SequenceNumber notified);
  DBImpl* const db_;
  std::thread thread_;
  std::atomic<bool> stop_{false};
  std::atomic<size_t> waiters_{0};
  AsyncCommitEvent waiter_event_;
  AsyncCommitEvent progress_event_;
};
}  // namespace ROCKSDB_NAMESPACE
