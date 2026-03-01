#pragma once
#include <utility>
#include "onebase/common/rid.h"
#include "onebase/common/types.h"

namespace onebase {

class BufferPoolManager;

#define BPLUSTREE_ITERATOR_TYPE BPlusTreeIterator<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeIterator {
 public:
  BPlusTreeIterator() = default;
  BPlusTreeIterator(page_id_t page_id, int index, BufferPoolManager *bpm = nullptr);

  auto IsEnd() const -> bool;
  auto operator*() -> const std::pair<KeyType, ValueType> &;
  auto operator++() -> BPlusTreeIterator &;
  auto operator==(const BPlusTreeIterator &other) const -> bool;
  auto operator!=(const BPlusTreeIterator &other) const -> bool;

 private:
  page_id_t page_id_{INVALID_PAGE_ID};
  int index_{0};
  BufferPoolManager *bpm_{nullptr};
  std::pair<KeyType, ValueType> current_;
};

}  // namespace onebase
