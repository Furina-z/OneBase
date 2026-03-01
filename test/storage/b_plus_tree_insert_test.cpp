#include <gtest/gtest.h>
#include <functional>
#include "onebase/common/exception.h"
#include "onebase/common/rid.h"
#include "onebase/storage/index/b_plus_tree.h"

namespace onebase {

TEST(BPlusTreeInsertTest, InsertThrows) {
  // B+ tree requires a BPM; test that Insert throws NotImplementedException
  // without a real BPM (student must implement first)
  // This is a placeholder — students should expand after implementing BPM + B+ tree
  SUCCEED() << "B+ tree insert tests require a working BufferPoolManager. "
            << "Implement Lab 1 first, then run these tests.";
}

// Students: After implementing B+ tree, add tests for:
// - Insert single key
// - Insert multiple keys (ascending, descending, random order)
// - Insert with leaf split
// - Insert with internal split
// - Duplicate key handling
// - GetValue after Insert

}  // namespace onebase
