#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <future>
#include <thread>
#include <vector>

#include "eval/grading.h"

#include "onebase/common/rid.h"
#include "onebase/concurrency/lock_manager.h"
#include "onebase/concurrency/transaction.h"

namespace onebase {

// ============================================================
// Shared Lock Tests (20 pts)
// ============================================================

GRADED_TEST(Lab4Lock, SharedLockBasic, 10) {
  LockManager lm;
  Transaction txn(0);
  RID rid(0, 0);

  EXPECT_TRUE(lm.LockShared(&txn, rid));
  EXPECT_TRUE(txn.IsSharedLocked(rid));
  EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

  EXPECT_TRUE(lm.Unlock(&txn, rid));
  EXPECT_FALSE(txn.IsSharedLocked(rid));
  EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
}

GRADED_TEST(Lab4Lock, SharedLockCompat, 10) {
  LockManager lm;
  Transaction txn0(0), txn1(1);
  RID rid(0, 0);

  // Both should acquire S lock without blocking
  EXPECT_TRUE(lm.LockShared(&txn0, rid));
  EXPECT_TRUE(lm.LockShared(&txn1, rid));

  EXPECT_TRUE(txn0.IsSharedLocked(rid));
  EXPECT_TRUE(txn1.IsSharedLocked(rid));

  lm.Unlock(&txn0, rid);
  lm.Unlock(&txn1, rid);
}

// ============================================================
// Exclusive Lock Tests (25 pts)
// ============================================================

GRADED_TEST(Lab4Lock, ExclusiveLockBasic, 10) {
  LockManager lm;
  Transaction txn(0);
  RID rid(0, 0);

  EXPECT_TRUE(lm.LockExclusive(&txn, rid));
  EXPECT_TRUE(txn.IsExclusiveLocked(rid));
  EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

  lm.Unlock(&txn, rid);
  EXPECT_FALSE(txn.IsExclusiveLocked(rid));
}

GRADED_TEST(Lab4Lock, ExclusiveLockBlock, 15) {
  LockManager lm;
  Transaction txn0(0), txn1(1);
  RID rid(0, 0);

  lm.LockExclusive(&txn0, rid);

  std::promise<void> started;
  std::promise<bool> result;

  std::thread t([&]() {
    try {
      started.set_value();
      bool r = lm.LockShared(&txn1, rid);
      result.set_value(r);
    } catch (...) {
      result.set_value(false);
    }
  });

  started.get_future().wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // txn1 should be blocked (not yet holding S lock)
  EXPECT_FALSE(txn1.IsSharedLocked(rid));

  // Unlock txn0 — txn1 should unblock and acquire S lock
  lm.Unlock(&txn0, rid);
  EXPECT_TRUE(result.get_future().get());
  t.join();

  EXPECT_TRUE(txn1.IsSharedLocked(rid));
  lm.Unlock(&txn1, rid);
}

// ============================================================
// Lock Upgrade (15 pts)
// ============================================================

GRADED_TEST(Lab4Lock, LockUpgrade, 15) {
  LockManager lm;
  Transaction txn0(0);
  RID rid(0, 0);

  // Acquire S lock, then upgrade to X
  EXPECT_TRUE(lm.LockShared(&txn0, rid));
  EXPECT_TRUE(txn0.IsSharedLocked(rid));

  EXPECT_TRUE(lm.LockUpgrade(&txn0, rid));
  EXPECT_TRUE(txn0.IsExclusiveLocked(rid));
  EXPECT_FALSE(txn0.IsSharedLocked(rid));

  lm.Unlock(&txn0, rid);
}

// ============================================================
// Unlock and Notify (10 pts)
// ============================================================

GRADED_TEST(Lab4Lock, UnlockAndNotify, 10) {
  LockManager lm;
  Transaction txn0(0), txn1(1), txn2(2);
  RID rid(0, 0);

  lm.LockExclusive(&txn0, rid);

  std::atomic<int> acquired{0};

  auto lock_fn = [&](Transaction *txn) {
    try {
      lm.LockShared(txn, rid);
      acquired.fetch_add(1);
    } catch (...) {
    }
  };

  std::thread t1(lock_fn, &txn1);
  std::thread t2(lock_fn, &txn2);

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(acquired.load(), 0);  // Both blocked

  lm.Unlock(&txn0, rid);

  t1.join();
  t2.join();

  // Both should have acquired S locks
  EXPECT_EQ(acquired.load(), 2);
  EXPECT_TRUE(txn1.IsSharedLocked(rid));
  EXPECT_TRUE(txn2.IsSharedLocked(rid));

  lm.Unlock(&txn1, rid);
  lm.Unlock(&txn2, rid);
}

// ============================================================
// Two-Phase Locking Enforcement (15 pts)
// ============================================================

GRADED_TEST(Lab4Lock, TwoPLEnforce, 15) {
  LockManager lm;
  Transaction txn(0);
  RID rid1(0, 0), rid2(0, 1);

  // Acquire and release → enters SHRINKING
  lm.LockShared(&txn, rid1);
  lm.Unlock(&txn, rid1);
  EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);

  // Attempting to lock again in SHRINKING should fail and abort
  EXPECT_FALSE(lm.LockShared(&txn, rid2));
  EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);

  // Also test exclusive lock request in SHRINKING
  Transaction txn2(1);
  lm.LockExclusive(&txn2, rid1);
  lm.Unlock(&txn2, rid1);
  EXPECT_EQ(txn2.GetState(), TransactionState::SHRINKING);
  EXPECT_FALSE(lm.LockExclusive(&txn2, rid2));
  EXPECT_EQ(txn2.GetState(), TransactionState::ABORTED);
}

// ============================================================
// Multiple Resources (15 pts)
// ============================================================

GRADED_TEST(Lab4Lock, MultiResource, 15) {
  LockManager lm;
  Transaction txn0(0), txn1(1);
  RID rid1(0, 0), rid2(0, 1);

  // txn0 holds X lock on rid1
  EXPECT_TRUE(lm.LockExclusive(&txn0, rid1));

  // txn1 should be able to lock rid2 independently (no blocking)
  EXPECT_TRUE(lm.LockExclusive(&txn1, rid2));

  EXPECT_TRUE(txn0.IsExclusiveLocked(rid1));
  EXPECT_FALSE(txn0.IsExclusiveLocked(rid2));
  EXPECT_TRUE(txn1.IsExclusiveLocked(rid2));
  EXPECT_FALSE(txn1.IsExclusiveLocked(rid1));

  // Both can hold X locks on different resources simultaneously
  lm.Unlock(&txn0, rid1);
  lm.Unlock(&txn1, rid2);

  // Now test: txn0 holds X on rid1, txn1 tries X on rid1 (blocks),
  // but txn1 can still hold its own lock on rid2 once acquired
  Transaction txn2(2), txn3(3);
  lm.LockExclusive(&txn2, rid1);
  lm.LockExclusive(&txn3, rid2);

  // txn3 has rid2 X lock while txn2 has rid1 X lock — independent
  EXPECT_TRUE(txn2.IsExclusiveLocked(rid1));
  EXPECT_TRUE(txn3.IsExclusiveLocked(rid2));

  lm.Unlock(&txn2, rid1);
  lm.Unlock(&txn3, rid2);
}

}  // namespace onebase
