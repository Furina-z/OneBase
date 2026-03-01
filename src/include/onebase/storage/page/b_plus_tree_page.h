#pragma once
#include "onebase/common/types.h"

namespace onebase {

enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

class BPlusTreePage {
 public:
  auto IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
  auto IsRootPage() const -> bool { return parent_page_id_ == INVALID_PAGE_ID; }
  void SetPageType(IndexPageType page_type) { page_type_ = page_type; }
  auto GetPageType() const -> IndexPageType { return page_type_; }

  auto GetSize() const -> int { return size_; }
  void SetSize(int size) { size_ = size; }
  void IncreaseSize(int amount) { size_ += amount; }

  auto GetMaxSize() const -> int { return max_size_; }
  void SetMaxSize(int max_size) { max_size_ = max_size; }
  auto GetMinSize() const -> int { return IsLeafPage() ? (max_size_ / 2) : ((max_size_ + 1) / 2); }

  auto GetParentPageId() const -> page_id_t { return parent_page_id_; }
  void SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

 private:
  IndexPageType page_type_{IndexPageType::INVALID_INDEX_PAGE};
  int size_{0};
  int max_size_{0};
  page_id_t parent_page_id_{INVALID_PAGE_ID};
};

}  // namespace onebase
