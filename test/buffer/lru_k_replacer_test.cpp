#include <gtest/gtest.h>
#include "onebase/buffer/lru_k_replacer.h"
#include "onebase/common/exception.h"

namespace onebase {

TEST(LRUKReplacerTest, BasicEviction) {
  // This test will throw NotImplementedException until student implements it
  LRUKReplacer replacer(7, 2);

  // Test that methods throw NotImplementedException
  frame_id_t frame;
  EXPECT_THROW(replacer.Evict(&frame), NotImplementedException);
  EXPECT_THROW(replacer.RecordAccess(1), NotImplementedException);
  EXPECT_THROW(replacer.SetEvictable(1, true), NotImplementedException);
  EXPECT_THROW(replacer.Remove(1), NotImplementedException);
  EXPECT_THROW(replacer.Size(), NotImplementedException);
}

// Students: After implementing LRU-K, add more comprehensive tests here:
// - Test with k=2, verify backward k-distance ordering
// - Test eviction with mix of evictable/non-evictable frames
// - Test that frames with <k accesses are evicted before frames with k accesses
// - Test Remove() on non-evictable frame throws

}  // namespace onebase
