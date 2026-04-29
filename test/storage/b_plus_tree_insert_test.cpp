#include "b_plus_tree_test_common.h"

namespace onebase {

using onebase::test::BPlusTreeTest;

TEST_F(BPlusTreeTest, InsertLookupAndDuplicateHandling) { VerifyInsertLookupAndDuplicateHandling(); }

TEST_F(BPlusTreeTest, InsertionsSplitAndIterationIsOrdered) { VerifyInsertionsSplitAndIterationIsOrdered(); }

TEST_F(BPlusTreeTest, BeginFromKeyAndSparseLookupsWork) { VerifyBeginFromKeyAndSparseLookupsWork(); }

}  // namespace onebase
