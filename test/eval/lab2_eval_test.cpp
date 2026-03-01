#include <gtest/gtest.h>
#include <algorithm>
#include <functional>
#include <string>
#include <vector>

#include "eval/grading.h"

#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/rid.h"
#include "onebase/storage/disk/disk_manager.h"
#include "onebase/storage/index/b_plus_tree.h"
#include "onebase/storage/index/b_plus_tree_iterator.h"

namespace onebase {

class BPlusTreeEvalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    db_name_ = "__eval_bpt_" + std::to_string(reinterpret_cast<uintptr_t>(this)) + ".db";
    disk_manager_ = new DiskManager(db_name_);
    bpm_ = new BufferPoolManager(200, disk_manager_);
  }
  void TearDown() override {
    delete bpm_;
    disk_manager_->ShutDown();
    delete disk_manager_;
    std::remove(db_name_.c_str());
  }

  std::string db_name_;
  DiskManager *disk_manager_{nullptr};
  BufferPoolManager *bpm_{nullptr};
};

// ============================================================
// Insert Tests (50 pts)
// ============================================================

GRADED_TEST_F(BPlusTreeEvalTest, SingleInsertLookup, 10) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 4, 4);

  RID rid(1, 1);
  EXPECT_TRUE(tree.Insert(5, rid));

  std::vector<RID> result;
  EXPECT_TRUE(tree.GetValue(5, &result));
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], rid);
}

GRADED_TEST_F(BPlusTreeEvalTest, MultipleInserts, 10) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 4, 4);

  std::vector<int> keys = {7, 3, 9, 1, 5, 8, 2, 6, 4, 10};
  for (auto k : keys) {
    EXPECT_TRUE(tree.Insert(k, RID(k, k)));
  }

  for (auto k : keys) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(k, &result));
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], RID(k, k));
  }

  // Key not inserted
  std::vector<RID> result;
  EXPECT_FALSE(tree.GetValue(99, &result));
}

GRADED_TEST_F(BPlusTreeEvalTest, LeafSplit, 10) {
  // leaf_max_size=3: leaf splits after 3 keys
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 3, 5);

  for (int i = 1; i <= 5; i++) {
    EXPECT_TRUE(tree.Insert(i, RID(i, i)));
  }

  // All keys still reachable after split
  for (int i = 1; i <= 5; i++) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(i, &result));
    ASSERT_EQ(result.size(), 1u);
  }
}

GRADED_TEST_F(BPlusTreeEvalTest, InternalSplit, 10) {
  // Small sizes force internal splits
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 3, 3);

  for (int i = 1; i <= 20; i++) {
    EXPECT_TRUE(tree.Insert(i, RID(i, i)));
  }

  for (int i = 1; i <= 20; i++) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(i, &result));
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0], RID(i, i));
  }
}

GRADED_TEST_F(BPlusTreeEvalTest, DuplicateKey, 5) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 4, 4);

  EXPECT_TRUE(tree.Insert(42, RID(1, 1)));
  EXPECT_FALSE(tree.Insert(42, RID(2, 2)));  // duplicate

  std::vector<RID> result;
  EXPECT_TRUE(tree.GetValue(42, &result));
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], RID(1, 1));  // original value
}

GRADED_TEST_F(BPlusTreeEvalTest, LargeScale, 5) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 5, 5);

  for (int i = 0; i < 500; i++) {
    EXPECT_TRUE(tree.Insert(i, RID(i, i)));
  }

  for (int i = 0; i < 500; i++) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(i, &result)) << "Missing key: " << i;
    ASSERT_EQ(result.size(), 1u);
  }
}

// ============================================================
// Delete Tests (50 pts)
// ============================================================

GRADED_TEST_F(BPlusTreeEvalTest, SimpleDelete, 10) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 4, 4);

  for (int i = 1; i <= 5; i++) {
    tree.Insert(i, RID(i, i));
  }

  tree.Remove(3);

  std::vector<RID> result;
  EXPECT_FALSE(tree.GetValue(3, &result));

  // Others still present
  for (int i : {1, 2, 4, 5}) {
    result.clear();
    EXPECT_TRUE(tree.GetValue(i, &result));
    ASSERT_EQ(result.size(), 1u);
  }
}

GRADED_TEST_F(BPlusTreeEvalTest, DeleteMerge, 10) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 3, 3);

  for (int i = 1; i <= 10; i++) {
    tree.Insert(i, RID(i, i));
  }

  // Delete enough to trigger merges
  for (int i = 1; i <= 7; i++) {
    tree.Remove(i);
  }

  // Remaining keys still accessible
  for (int i = 8; i <= 10; i++) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(i, &result));
    ASSERT_EQ(result.size(), 1u);
  }

  // Deleted keys gone
  for (int i = 1; i <= 7; i++) {
    std::vector<RID> result;
    EXPECT_FALSE(tree.GetValue(i, &result));
  }
}

GRADED_TEST_F(BPlusTreeEvalTest, DeleteRedistribute, 10) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 4, 4);

  for (int i = 1; i <= 10; i++) {
    tree.Insert(i, RID(i, i));
  }

  // Delete from one leaf to trigger redistribution (not merge)
  tree.Remove(1);
  tree.Remove(2);

  for (int i = 3; i <= 10; i++) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(i, &result));
    ASSERT_EQ(result.size(), 1u);
  }
}

GRADED_TEST_F(BPlusTreeEvalTest, MixedInsertDelete, 10) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 4, 4);

  // Insert 1..10
  for (int i = 1; i <= 10; i++) {
    tree.Insert(i, RID(i, i));
  }

  // Delete evens
  for (int i = 2; i <= 10; i += 2) {
    tree.Remove(i);
  }

  // Insert 11..15
  for (int i = 11; i <= 15; i++) {
    tree.Insert(i, RID(i, i));
  }

  // Delete some odds
  tree.Remove(1);
  tree.Remove(3);
  tree.Remove(5);

  // Remaining: 7, 9, 11, 12, 13, 14, 15
  std::vector<int> expected = {7, 9, 11, 12, 13, 14, 15};
  for (int k : expected) {
    std::vector<RID> result;
    EXPECT_TRUE(tree.GetValue(k, &result)) << "Missing key: " << k;
  }

  // Deleted should be gone
  for (int k : {1, 2, 3, 4, 5, 6, 8, 10}) {
    std::vector<RID> result;
    EXPECT_FALSE(tree.GetValue(k, &result)) << "Key should be deleted: " << k;
  }
}

GRADED_TEST_F(BPlusTreeEvalTest, DeleteAll, 5) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 3, 3);

  for (int i = 1; i <= 20; i++) {
    tree.Insert(i, RID(i, i));
  }

  for (int i = 1; i <= 20; i++) {
    tree.Remove(i);
  }

  EXPECT_TRUE(tree.IsEmpty());

  for (int i = 1; i <= 20; i++) {
    std::vector<RID> result;
    EXPECT_FALSE(tree.GetValue(i, &result));
  }
}

GRADED_TEST_F(BPlusTreeEvalTest, IteratorOrder, 5) {
  BPlusTree<int, RID, std::less<int>> tree("test", bpm_, std::less<int>{}, 4, 4);

  std::vector<int> keys = {5, 3, 8, 1, 9, 2, 7, 4, 6, 10};
  for (auto k : keys) {
    tree.Insert(k, RID(k, k));
  }

  // Iterator should return keys in sorted ascending order
  std::sort(keys.begin(), keys.end());

  int idx = 0;
  for (auto it = tree.Begin(); it != tree.End(); ++it) {
    ASSERT_LT(idx, static_cast<int>(keys.size()));
    auto [key, rid] = *it;
    EXPECT_EQ(key, keys[idx]) << "Iterator order mismatch at position " << idx;
    idx++;
  }
  EXPECT_EQ(idx, static_cast<int>(keys.size()));
}

}  // namespace onebase
