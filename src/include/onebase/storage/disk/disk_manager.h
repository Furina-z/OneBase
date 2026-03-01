#pragma once
#include <fstream>
#include <string>
#include <mutex>
#include "onebase/common/config.h"
#include "onebase/common/types.h"

namespace onebase {

class DiskManager {
 public:
  explicit DiskManager(const std::string &db_file);
  ~DiskManager();

  void ReadPage(page_id_t page_id, char *page_data);
  void WritePage(page_id_t page_id, const char *page_data);

  auto AllocatePage() -> page_id_t;
  void DeallocatePage(page_id_t page_id);

  auto GetNumPages() const -> size_t { return num_pages_; }
  void ShutDown();

 private:
  std::string db_file_;
  std::fstream db_io_;
  page_id_t next_page_id_{0};
  size_t num_pages_{0};
  std::mutex db_io_latch_;
};

}  // namespace onebase
