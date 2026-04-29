#include "lab1_test_common.h"

namespace onebase {

using onebase::test::PageGuardLab1Test;

TEST_F(PageGuardLab1Test, BasicGuardUnpin) { VerifyBasicGuardUnpin(); }

TEST_F(PageGuardLab1Test, ReadGuardLatch) { VerifyReadGuardLatch(); }

TEST_F(PageGuardLab1Test, WriteGuardLatch) { VerifyWriteGuardLatch(); }

TEST_F(PageGuardLab1Test, MoveSemantics) { VerifyMoveSemantics(); }

}  // namespace onebase
