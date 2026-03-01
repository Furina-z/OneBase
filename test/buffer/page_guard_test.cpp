#include <gtest/gtest.h>
#include "onebase/buffer/page_guard.h"
#include "onebase/common/exception.h"

namespace onebase {

TEST(PageGuardTest, BasicPageGuardDropNoOpWhenEmpty) {
  // Default-constructed guard has null page — Drop should be a safe no-op
  BasicPageGuard guard;
  EXPECT_NO_THROW(guard.Drop());
}

// Students: After implementing PageGuard, add tests for:
// - BasicPageGuard RAII (auto unpin on destruction)
// - ReadPageGuard holds read latch, releases on Drop
// - WritePageGuard holds write latch, releases on Drop
// - Move semantics transfer ownership correctly
// - Drop with a real BufferPoolManager throws NotImplementedException until implemented

}  // namespace onebase
