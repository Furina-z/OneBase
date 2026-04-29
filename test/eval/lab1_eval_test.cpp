#include "eval/grading.h"
#include "buffer/lab1_test_common.h"

namespace onebase {

using onebase::test::BufferPoolManagerLab1Test;
using onebase::test::LRUKReplacerLab1Test;
using onebase::test::PageGuardLab1Test;

GRADED_TEST_F(LRUKReplacerLab1Test, RecordAccessAndSize, 5) { VerifyRecordAccessAndSize(); }

GRADED_TEST_F(LRUKReplacerLab1Test, BasicEvict, 5) { VerifyBasicEvict(); }

GRADED_TEST_F(LRUKReplacerLab1Test, EvictByInfDistance, 5) { VerifyEvictByInfDistance(); }

GRADED_TEST_F(LRUKReplacerLab1Test, EvictByKDistance, 10) { VerifyEvictByKDistance(); }

GRADED_TEST_F(LRUKReplacerLab1Test, SetEvictableAndRemove, 5) { VerifySetEvictableAndRemove(); }

GRADED_TEST_F(BufferPoolManagerLab1Test, NewPageBasic, 5) { VerifyNewPageBasic(); }

GRADED_TEST_F(BufferPoolManagerLab1Test, FetchPageBasic, 5) { VerifyFetchPageBasic(); }

GRADED_TEST_F(BufferPoolManagerLab1Test, UnpinAndEvict, 10) { VerifyUnpinAndEvict(); }

GRADED_TEST_F(BufferPoolManagerLab1Test, DirtyPagePersistence, 10) { VerifyDirtyPagePersistence(); }

GRADED_TEST_F(BufferPoolManagerLab1Test, DeletePage, 5) { VerifyDeletePage(); }

GRADED_TEST_F(BufferPoolManagerLab1Test, FlushPage, 5) { VerifyFlushPage(); }

GRADED_TEST_F(PageGuardLab1Test, BasicGuardUnpin, 10) { VerifyBasicGuardUnpin(); }

GRADED_TEST_F(PageGuardLab1Test, ReadGuardLatch, 10) { VerifyReadGuardLatch(); }

GRADED_TEST_F(PageGuardLab1Test, WriteGuardLatch, 5) { VerifyWriteGuardLatch(); }

GRADED_TEST_F(PageGuardLab1Test, MoveSemantics, 5) { VerifyMoveSemantics(); }

}  // namespace onebase
