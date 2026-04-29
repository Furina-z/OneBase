#include "b_plus_tree_test_common.h"

namespace onebase {

using onebase::test::BPlusTreeTest;

TEST_F(BPlusTreeTest, DeleteMaintainsCorrectnessAndCanEmptyTree) {
  VerifyDeleteMaintainsCorrectnessAndCanEmptyTree();
}

}  // namespace onebase
