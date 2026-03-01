#pragma once
#include <functional>
#include <utility>
#include "onebase/common/rid.h"
#include "onebase/storage/page/b_plus_tree_page.h"

namespace onebase {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  void Init(int max_size);
  auto GetNextPageId() const -> page_id_t { return next_page_id_; }
  void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;

  auto KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int;
  auto Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const -> bool;
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int;
  auto RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) -> int;

  void MoveHalfTo(BPlusTreeLeafPage *recipient);
  void MoveAllTo(BPlusTreeLeafPage *recipient);
  void MoveFirstToEndOf(BPlusTreeLeafPage *recipient);
  void MoveLastToFrontOf(BPlusTreeLeafPage *recipient);

 private:
  page_id_t next_page_id_{INVALID_PAGE_ID};
  std::pair<KeyType, ValueType> array_[0];
};

}  // namespace onebase
