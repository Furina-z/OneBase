#pragma once
#include <functional>
#include <utility>
#include "onebase/storage/page/b_plus_tree_page.h"

namespace onebase {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>

template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  void Init(int max_size);
  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);

  auto ValueIndex(const ValueType &value) const -> int;
  auto Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  void PopulateNewRoot(const ValueType &old_value, const KeyType &key, const ValueType &new_value);
  auto InsertNodeAfter(const ValueType &old_value, const KeyType &key, const ValueType &new_value) -> int;
  void Remove(int index);
  auto RemoveAndReturnOnlyChild() -> ValueType;

  void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key);
  void MoveHalfTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key);
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key);
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key);

 private:
  std::pair<KeyType, ValueType> array_[0];
};

}  // namespace onebase
