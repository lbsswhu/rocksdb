// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
#include "util/async_commit_queue.h"
#include "util/async_commit_event.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <future>
#include <thread>
#include <vector>

#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
TEST(AsyncCommitQueueTest, SupportsCxx11HeapAllocation) {
  // Ordinary C++11 new does not guarantee extended alignment.
  EXPECT_LE(alignof(AsyncCommitQueue<uint64_t>), alignof(std::max_align_t));
}

TEST(AsyncCommitQueueTest, EmptyAndPublishedChainsAreFIFO) {
  AsyncCommitQueue<uint64_t> q(1);
  uint64_t value = 999;
  EXPECT_FALSE(q.TryPop(0, &value));
  for (uint64_t i = 0; i < 1000; i += 2) {
    auto* first = new AsyncCommitQueue<uint64_t>::Node(i);
    auto* last = new AsyncCommitQueue<uint64_t>::Node(i + 1);
    first->next.store(last);
    q.PushChain(first, last);
  }
  for (uint64_t i = 0; i < 1000; ++i) {
    ASSERT_TRUE(q.TryPop(0, &value));
    ASSERT_EQ(i, value);
  }
  EXPECT_FALSE(q.TryPop(0, &value));
  q.Reclaim(0);
  EXPECT_EQ(0U, q.RetiredCount());
}

TEST(AsyncCommitQueueTest, MultipleConsumersExactlyOnceAcrossChains) {
  const size_t count = 50000, workers = 4;
  AsyncCommitQueue<uint64_t> q(workers);
  std::vector<std::atomic<int>> seen(count);
  for (auto& value : seen) value.store(0);
  std::atomic<size_t> done{0};
  std::atomic<bool> finished{false};
  std::vector<std::thread> readers;
  for (size_t worker = 0; worker < workers; ++worker) {
    readers.emplace_back([&, worker] {
      uint64_t value;
      while (!finished.load() || done.load() < count) {
        if (q.TryPop(worker, &value)) {
          ASSERT_LT(value, count);
          ++seen[value];
          ++done;
        } else {
          std::this_thread::yield();
        }
      }
      q.Reclaim(worker);
    });
  }
  for (size_t i = 0; i < count; i += 5) {
    AsyncCommitQueue<uint64_t>::Node *first = nullptr, *last = nullptr;
    for (size_t n = i; n < i + 5; ++n) {
      auto* node = new AsyncCommitQueue<uint64_t>::Node(n);
      if (last) last->next.store(node);
      else first = node;
      last = node;
    }
    q.PushChain(first, last);
  }
  finished.store(true);
  for (auto& reader : readers) reader.join();
  ASSERT_EQ(count, done.load());
  for (auto& value : seen) ASSERT_EQ(1, value.load());
  for (size_t worker = 0; worker < workers; ++worker) q.Reclaim(worker);
  EXPECT_EQ(0U, q.RetiredCount());
}

static void PauseReaderAt(const char* hook, bool protected_head_expected) {
  AsyncCommitQueue<uint64_t> q(2);
  auto* first = new AsyncCommitQueue<uint64_t>::Node(1);
  q.PushChain(first, first);
  std::promise<void> protected_head, resume;
  auto resumed = resume.get_future().share();
  std::atomic<bool> once{true};
  auto* sp = SyncPoint::GetInstance();
  sp->SetCallBack(hook, [&](void*) {
    if (once.exchange(false)) {
      protected_head.set_value();
      resumed.wait();
    }
  });
  sp->EnableProcessing();
  std::thread paused([&] {
    uint64_t value;
    EXPECT_FALSE(q.TryPop(0, &value));
  });
  protected_head.get_future().wait();
  uint64_t value = 0;
  EXPECT_TRUE(q.TryPop(1, &value));
  EXPECT_EQ(1U, value);
  for (uint64_t i = 2; i < 2000; ++i) {
    auto* node = new AsyncCommitQueue<uint64_t>::Node(i);
    q.PushChain(node, node);
    EXPECT_TRUE(q.TryPop(1, &value));
    EXPECT_EQ(i, value);
  }
  q.Reclaim(1);
  EXPECT_EQ(protected_head_expected ? 1U : 0U, q.RetiredCount());
  resume.set_value();
  paused.join();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  q.Reclaim(0);
  q.Reclaim(1);
  EXPECT_EQ(0U, q.RetiredCount());
}

TEST(AsyncCommitQueueTest, PausedHazardReaderDoesNotBlockOtherConsumers) {
  PauseReaderAt("AsyncCommitQueue::Pop:ProtectedHead", true);
}

TEST(AsyncCommitQueueTest, RevalidatesAfterLoadBeforePublishingHazard) {
  PauseReaderAt("AsyncCommitQueue::Pop:LoadedHead", false);
}

TEST(AsyncCommitEventTest, NotificationBeforeKernelWaitIsNotLost) {
  AsyncCommitEvent event;
  for (int i = 0; i < 100; ++i) {
    const auto epoch = event.PrepareWait();
    event.NotifyAll();
    std::promise<void> returned;
    std::thread waiter([&] { event.Wait(epoch); returned.set_value(); });
    const bool ready = returned.get_future().wait_for(std::chrono::seconds(1)) ==
                       std::future_status::ready;
    if (!ready) event.NotifyAll();
    waiter.join();
    event.CancelWait();
    ASSERT_TRUE(ready);
  }
}

TEST(AsyncCommitEventTest, BroadcastWakesAllRegisteredWorkers) {
  AsyncCommitEvent event;
  std::atomic<int> ready{0}, returned{0};
  std::vector<std::thread> waiters;
  for (int i = 0; i < 4; ++i) {
    waiters.emplace_back([&] {
      const auto epoch = event.PrepareWait();
      ++ready;
      event.Wait(epoch);
      event.CancelWait();
      ++returned;
    });
  }
  while (ready.load() != 4) std::this_thread::yield();
  event.NotifyAll();
  for (auto& waiter : waiters) waiter.join();
  EXPECT_EQ(4, returned.load());
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
