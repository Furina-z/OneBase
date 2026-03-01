#include <gtest/gtest.h>
#include "onebase/common/exception.h"
#include "onebase/concurrency/lock_manager.h"
#include "onebase/concurrency/transaction.h"

namespace onebase {

TEST(LockManagerTest, SharedLockThrows) {
  LockManager lock_mgr;
  Transaction txn(0);
  RID rid(0, 0);

  EXPECT_THROW(lock_mgr.LockShared(&txn, rid), NotImplementedException);
}

TEST(LockManagerTest, ExclusiveLockThrows) {
  LockManager lock_mgr;
  Transaction txn(0);
  RID rid(0, 0);

  EXPECT_THROW(lock_mgr.LockExclusive(&txn, rid), NotImplementedException);
}

// Students: After implementing LockManager, add tests for:
// - Shared lock compatibility (multiple shared locks on same rid)
// - Exclusive lock exclusion (exclusive blocks shared and vice versa)
// - Lock upgrade (shared -> exclusive)
// - 2PL enforcement (no locks after SHRINKING state)
// - Deadlock scenarios (if implementing deadlock detection)

}  // namespace onebase
