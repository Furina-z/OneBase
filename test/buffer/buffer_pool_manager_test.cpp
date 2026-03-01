#include <gtest/gtest.h>
#include <cstring>
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/exception.h"
#include "onebase/storage/disk/disk_manager.h"

namespace onebase {

TEST(BufferPoolManagerTest, NewPageThrows) {
  const std::string db_name = "test_bpm.db";
  DiskManager disk_manager(db_name);
  BufferPoolManager bpm(10, &disk_manager);

  page_id_t page_id;
  EXPECT_THROW(bpm.NewPage(&page_id), NotImplementedException);

  disk_manager.ShutDown();
  std::remove(db_name.c_str());
}

TEST(BufferPoolManagerTest, FetchPageThrows) {
  const std::string db_name = "test_bpm_fetch.db";
  DiskManager disk_manager(db_name);
  BufferPoolManager bpm(10, &disk_manager);

  EXPECT_THROW(bpm.FetchPage(0), NotImplementedException);

  disk_manager.ShutDown();
  std::remove(db_name.c_str());
}

// Students: After implementing BPM, add tests for:
// - NewPage/FetchPage/UnpinPage cycle
// - Eviction when pool is full
// - Dirty page flush on eviction
// - DeletePage
// - FlushPage/FlushAllPages

}  // namespace onebase
