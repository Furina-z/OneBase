#include "lab4_test_common.h"

namespace onebase {

using onebase::test::LockManagerLab4Test;

TEST_F(LockManagerLab4Test, SharedLockBasic) { VerifySharedLockBasic(); }

TEST_F(LockManagerLab4Test, SharedLockCompat) { VerifySharedLockCompat(); }

TEST_F(LockManagerLab4Test, ExclusiveLockBasic) { VerifyExclusiveLockBasic(); }

TEST_F(LockManagerLab4Test, ExclusiveLockBlock) { VerifyExclusiveLockBlock(); }

TEST_F(LockManagerLab4Test, LockUpgrade) { VerifyLockUpgrade(); }

TEST_F(LockManagerLab4Test, UnlockAndNotify) { VerifyUnlockAndNotify(); }

TEST_F(LockManagerLab4Test, TwoPLEnforce) { VerifyTwoPLEnforce(); }

TEST_F(LockManagerLab4Test, MultiResource) { VerifyMultiResource(); }

}  // namespace onebase
