#include "onebase/storage/disk/disk_manager.h"
#include <cstring>
#include <stdexcept>
#include "onebase/common/logger.h"

namespace onebase {

DiskManager::DiskManager(const std::string &db_file) : db_file_(db_file) {
  db_io_.open(db_file_, std::ios::binary | std::ios::in | std::ios::out);
  if (!db_io_.is_open()) {
    db_io_.clear();
    db_io_.open(db_file_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!db_io_.is_open()) {
      throw std::runtime_error("Cannot open DB file: " + db_file_);
    }
  }
  db_io_.seekg(0, std::ios::end);
  auto file_size = db_io_.tellg();
  num_pages_ = static_cast<size_t>(file_size) / ONEBASE_PAGE_SIZE;
  next_page_id_ = static_cast<page_id_t>(num_pages_);
}

DiskManager::~DiskManager() { ShutDown(); }

void DiskManager::ShutDown() {
  if (db_io_.is_open()) {
    db_io_.close();
  }
}

void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
  std::scoped_lock lock(db_io_latch_);
  auto offset = static_cast<std::streamoff>(page_id) * ONEBASE_PAGE_SIZE;
  db_io_.seekg(offset);
  if (db_io_.bad()) {
    throw std::runtime_error("I/O error while seeking to read page");
  }
  db_io_.read(page_data, ONEBASE_PAGE_SIZE);
  auto read_count = db_io_.gcount();
  if (read_count < ONEBASE_PAGE_SIZE) {
    std::memset(page_data + read_count, 0, ONEBASE_PAGE_SIZE - read_count);
  }
}

void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
  std::scoped_lock lock(db_io_latch_);
  auto offset = static_cast<std::streamoff>(page_id) * ONEBASE_PAGE_SIZE;
  db_io_.seekp(offset);
  db_io_.write(page_data, ONEBASE_PAGE_SIZE);
  if (db_io_.bad()) {
    throw std::runtime_error("I/O error while writing page");
  }
  db_io_.flush();
  if (static_cast<size_t>(page_id) >= num_pages_) {
    num_pages_ = page_id + 1;
  }
}

auto DiskManager::AllocatePage() -> page_id_t {
  return next_page_id_++;
}

void DiskManager::DeallocatePage([[maybe_unused]] page_id_t page_id) {
  // No-op for now; a real system would maintain a free list
}

}  // namespace onebase
