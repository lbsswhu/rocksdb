// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <atomic>
#include <memory>
#include <thread>

#include "db/dbformat.h"
#include "util/async_commit_event.h"

namespace ROCKSDB_NAMESPACE {
class DBImpl;
struct AsyncCommitGroup;

// Single-worker progress only needs reader notification. Multiple workers
// transfer cleaned completion records here for continuous prefix publication.
class AsyncCommitSequenceNotifier {
 public:
  explicit AsyncCommitSequenceNotifier(DBImpl* db);
  ~AsyncCommitSequenceNotifier();
  void Start(SequenceNumber initial_sequence);
  void Stop();
  void PublicationAdvanced();
  void SubmitCompleted(const std::shared_ptr<AsyncCommitGroup>& group);
  void RegisterWaiter();
  void UnregisterWaiter();

 private:
  void Run(SequenceNumber notified);
  bool HasCompleted() const;
  void ProcessCompleted();
  DBImpl* const db_;
  const bool multi_worker_;
  std::atomic<AsyncCommitGroup*> completed_reports_{nullptr};
  AsyncCommitGroup* pending_reports_ = nullptr;  // notifier only
  std::shared_ptr<AsyncCommitGroup> completed_;  // notifier's ordered list
  std::thread thread_;
  std::atomic<bool> stop_{false};
  std::atomic<size_t> waiters_{0};
  AsyncCommitEvent waiter_event_;
  AsyncCommitEvent progress_event_;
};
}  // namespace ROCKSDB_NAMESPACE
