#include "eval/grading.h"
#include "concurrency/lab4_test_common.h"

namespace onebase {

using onebase::test::LockManagerLab4Test;

GRADED_TEST_F(LockManagerLab4Test, SharedLockBasic, 10) { VerifySharedLockBasic(); }

GRADED_TEST_F(LockManagerLab4Test, SharedLockCompat, 10) { VerifySharedLockCompat(); }

GRADED_TEST_F(LockManagerLab4Test, ExclusiveLockBasic, 10) { VerifyExclusiveLockBasic(); }

GRADED_TEST_F(LockManagerLab4Test, ExclusiveLockBlock, 15) { VerifyExclusiveLockBlock(); }

GRADED_TEST_F(LockManagerLab4Test, LockUpgrade, 15) { VerifyLockUpgrade(); }

GRADED_TEST_F(LockManagerLab4Test, UnlockAndNotify, 10) { VerifyUnlockAndNotify(); }

GRADED_TEST_F(LockManagerLab4Test, TwoPLEnforce, 15) { VerifyTwoPLEnforce(); }

GRADED_TEST_F(LockManagerLab4Test, MultiResource, 15) { VerifyMultiResource(); }

}  // namespace onebase
