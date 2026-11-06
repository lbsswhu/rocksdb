// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
// FIFO for one externally serialized producer and fixed, registered consumers.
// No operation allocates queue storage: callers prepare Node chains beforehand.
// Michael--Scott head/tail helping and sequentially consistent hazard pointers
// protect both the old dummy and the new head. Values stay immutable until node
// reclamation; a successful pop returns a copy independent of node lifetime.
// Queue progress does not depend on a paused producer/consumer holding a lock.
// Destruction requires all users to have stopped.
template <typename T>
class AsyncCommitQueue {
 public:
  struct Node {
    explicit Node(const T& item = T()) : value(item) {}
    std::atomic<Node*> next{nullptr};
    const T value;
  };

  explicit AsyncCommitQueue(size_t consumers)
      : participants_(new Participant[consumers + 1]),
        consumers_(consumers),
        reclaim_threshold_(std::max<size_t>(32, 4 * (consumers + 1))) {
    static_assert(ATOMIC_POINTER_LOCK_FREE == 2,
                  "async commit requires lock-free pointer atomics");
    Node* dummy = new Node;
    head_.store(dummy);
    tail_.store(dummy);
    for (size_t i = 0; i <= consumers_; ++i)
      participants_[i].retired.reserve(reclaim_threshold_ + 1);
  }
  ~AsyncCommitQueue() {
    Node* node = head_.load();
    while (node) {
      Node* next = node->next.load();
      delete node;
      node = next;
    }
    for (size_t i = 1; i <= consumers_; ++i)
      for (Node* retired : participants_[i].retired) delete retired;
  }
  AsyncCommitQueue(const AsyncCommitQueue&) = delete;
  AsyncCommitQueue& operator=(const AsyncCommitQueue&) = delete;

  void PushChain(Node* first, Node* last) {
    assert(first && last && last->next.load() == nullptr);
    auto& hazard = participants_[0].hazards[0];
    for (;;) {
      Node* tail = tail_.load();
      hazard.store(tail);
      if (tail != tail_.load()) continue;
      Node* next = tail->next.load();
      if (next) {
        tail_.compare_exchange_strong(tail, next);
        continue;
      }
      if (tail->next.compare_exchange_strong(next, first)) {
        // With one logical producer, last cannot retire before this returns:
        // it cannot acquire a successor until the next PushChain invocation.
        tail_.compare_exchange_strong(tail, last);
        hazard.store(nullptr);
        return;
      }
      ++cas_retries_;
    }
  }

  bool TryPop(size_t consumer, T* result) {
    assert(consumer < consumers_);
    auto& participant = participants_[consumer + 1];
    auto& hp = participant.hazards;
    for (;;) {
      Node* head = head_.load();
      TEST_SYNC_POINT("AsyncCommitQueue::Pop:LoadedHead");
      hp[0].store(head);
      if (head != head_.load()) continue;
      TEST_SYNC_POINT("AsyncCommitQueue::Pop:ProtectedHead");
      Node* next = head->next.load();
      hp[1].store(next);
      // Never dereference next before validating the protected head again.
      if (head != head_.load()) continue;
      if (!next) {
        hp[0].store(nullptr);
        hp[1].store(nullptr);
        return false;
      }
      Node* tail = tail_.load();
      if (head == tail) {
        tail_.compare_exchange_strong(tail, next);
        continue;
      }
      if (head_.compare_exchange_strong(head, next)) {
        *result = next->value;
        hp[0].store(nullptr);
        hp[1].store(nullptr);
        participant.retired.push_back(head);
        ++retired_count_;
        if (participant.retired.size() >= reclaim_threshold_) Reclaim(consumer);
        return true;
      }
      ++cas_retries_;
    }
  }

  // Only the registered consumer may mutate its own retirement list. At most
  // 2*(consumers+1) nodes can be hazard-protected; scanning at a larger threshold
  // bounds each list even when one participant is indefinitely paused.
  void Reclaim(size_t consumer) {
    auto& retired = participants_[consumer + 1].retired;
    size_t kept = 0;
    for (Node* node : retired) {
      bool protected_node = false;
      for (size_t i = 0; i <= consumers_ && !protected_node; ++i)
        for (auto& hazard : participants_[i].hazards)
          if (hazard.load() == node) protected_node = true;
      if (protected_node) {
        retired[kept++] = node;
      } else {
        delete node;
        --retired_count_;
      }
    }
    retired.resize(kept);
  }
  uint64_t CASRetries() const { return cas_retries_.load(); }
  uint64_t RetiredCount() const { return retired_count_.load(); }

 private:
  struct Participant {
    Participant() { for (auto& slot : hazards) slot.store(nullptr); }
    std::atomic<Node*> hazards[2];
    std::vector<Node*> retired;
  };
  std::unique_ptr<Participant[]> participants_;
  const size_t consumers_;
  const size_t reclaim_threshold_;
  std::atomic<Node*> head_{nullptr};
  // Separate producer/consumer cache traffic without requiring over-aligned
  // allocation, which ordinary C++11 new does not provide.
  char head_padding_[64];
  std::atomic<Node*> tail_{nullptr};
  std::atomic<uint64_t> cas_retries_{0}, retired_count_{0};
};
}  // namespace ROCKSDB_NAMESPACE
