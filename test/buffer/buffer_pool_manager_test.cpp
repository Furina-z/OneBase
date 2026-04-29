#include "lab1_test_common.h"

namespace onebase {

using onebase::test::BufferPoolManagerLab1Test;

TEST_F(BufferPoolManagerLab1Test, NewPageBasic) { VerifyNewPageBasic(); }

TEST_F(BufferPoolManagerLab1Test, FetchPageBasic) { VerifyFetchPageBasic(); }

TEST_F(BufferPoolManagerLab1Test, UnpinAndEvict) { VerifyUnpinAndEvict(); }

TEST_F(BufferPoolManagerLab1Test, DirtyPagePersistence) { VerifyDirtyPagePersistence(); }

TEST_F(BufferPoolManagerLab1Test, DeletePage) { VerifyDeletePage(); }

TEST_F(BufferPoolManagerLab1Test, FlushPage) { VerifyFlushPage(); }

}  // namespace onebase
