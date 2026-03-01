#include <gtest/gtest.h>
#include <functional>
#include "onebase/common/exception.h"
#include "onebase/common/rid.h"
#include "onebase/storage/index/b_plus_tree.h"

namespace onebase {

TEST(BPlusTreeDeleteTest, DeleteThrows) {
  SUCCEED() << "B+ tree delete tests require a working BufferPoolManager. "
            << "Implement Lab 1 and Lab 2 Insert first, then run these tests.";
}

// Students: After implementing B+ tree delete, add tests for:
// - Delete single key
// - Delete causing leaf merge
// - Delete causing redistribution
// - Delete all keys (tree becomes empty)
// - Iterator after deletion

}  // namespace onebase
