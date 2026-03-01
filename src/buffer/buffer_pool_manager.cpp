#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/common/exception.h"
#include "onebase/common/logger.h"

namespace onebase {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // TODO(student): Allocate a new page in the buffer pool
  // 1. Pick a victim frame from free list or replacer
  // 2. If victim is dirty, write it back to disk
  // 3. Allocate a new page_id via disk_manager_
  // 4. Update page_table_ and page metadata
  throw NotImplementedException("BufferPoolManager::NewPage");
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
  // TODO(student): Fetch a page from the buffer pool
  // 1. Search page_table_ for existing mapping
  // 2. If not found, pick a victim frame
  // 3. Read page from disk into the frame
  throw NotImplementedException("BufferPoolManager::FetchPage");
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
  // TODO(student): Unpin a page, decrementing pin count
  // - If pin_count reaches 0, set evictable in replacer
  throw NotImplementedException("BufferPoolManager::UnpinPage");
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // TODO(student): Delete a page from the buffer pool
  // - Page must have pin_count == 0
  // - Remove from page_table_, reset memory, add frame to free_list_
  throw NotImplementedException("BufferPoolManager::DeletePage");
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // TODO(student): Force flush a page to disk regardless of dirty flag
  throw NotImplementedException("BufferPoolManager::FlushPage");
}

void BufferPoolManager::FlushAllPages() {
  // TODO(student): Flush all pages in the buffer pool to disk
  throw NotImplementedException("BufferPoolManager::FlushAllPages");
}

}  // namespace onebase
