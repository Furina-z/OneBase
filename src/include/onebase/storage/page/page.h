#pragma once
#include <cstring>
#include "onebase/common/config.h"
#include "onebase/common/rwlatch.h"
#include "onebase/common/types.h"

namespace onebase {

class Page {
  friend class BufferPoolManager;

 public:
  Page() { ResetMemory(); }
  ~Page() = default;

  auto GetData() -> char * { return data_; }
  auto GetPageId() const -> page_id_t { return page_id_; }
  auto GetPinCount() const -> int { return pin_count_; }
  auto IsDirty() const -> bool { return is_dirty_; }

  void RLatch() { rwlatch_.RLock(); }
  void RUnlatch() { rwlatch_.RUnlock(); }
  void WLatch() { rwlatch_.WLock(); }
  void WUnlatch() { rwlatch_.WUnlock(); }

 protected:
  static constexpr size_t OFFSET_PAGE_START = 0;
  static constexpr size_t SIZE_PAGE_HEADER = 8;

 private:
  void ResetMemory() { std::memset(data_, 0, ONEBASE_PAGE_SIZE); }

  char data_[ONEBASE_PAGE_SIZE]{};
  page_id_t page_id_{INVALID_PAGE_ID};
  int pin_count_{0};
  bool is_dirty_{false};
  ReaderWriterLatch rwlatch_;
};

}  // namespace onebase
