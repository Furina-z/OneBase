#include <gtest/gtest.h>
#include <cstring>
#include "onebase/common/config.h"
#include "onebase/storage/disk/disk_manager.h"

namespace onebase {

TEST(DiskManagerTest, ReadWritePage) {
  const std::string db_name = "test_dm.db";
  DiskManager dm(db_name);

  char write_buf[ONEBASE_PAGE_SIZE];
  char read_buf[ONEBASE_PAGE_SIZE];

  std::memset(write_buf, 0, ONEBASE_PAGE_SIZE);
  std::strcpy(write_buf, "Hello OneBase!");

  auto page_id = dm.AllocatePage();
  EXPECT_EQ(page_id, 0);

  dm.WritePage(page_id, write_buf);
  dm.ReadPage(page_id, read_buf);

  EXPECT_EQ(std::strcmp(write_buf, read_buf), 0);

  dm.ShutDown();
  std::remove(db_name.c_str());
}

TEST(DiskManagerTest, MultiplePages) {
  const std::string db_name = "test_dm_multi.db";
  DiskManager dm(db_name);

  constexpr int num_pages = 5;
  char buf[ONEBASE_PAGE_SIZE];

  for (int i = 0; i < num_pages; ++i) {
    auto pid = dm.AllocatePage();
    EXPECT_EQ(pid, i);
    std::memset(buf, 0, ONEBASE_PAGE_SIZE);
    std::snprintf(buf, ONEBASE_PAGE_SIZE, "Page %d", i);
    dm.WritePage(pid, buf);
  }

  for (int i = 0; i < num_pages; ++i) {
    dm.ReadPage(i, buf);
    char expected[32];
    std::snprintf(expected, sizeof(expected), "Page %d", i);
    EXPECT_EQ(std::strcmp(buf, expected), 0);
  }

  dm.ShutDown();
  std::remove(db_name.c_str());
}

}  // namespace onebase
