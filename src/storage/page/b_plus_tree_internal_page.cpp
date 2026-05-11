#include "onebase/storage/page/b_plus_tree_internal_page.h"

#include <functional>

#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeInternalPage<int, page_id_t, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }

  return -1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // Internal page convention:
  // array_[0].first is invalid.
  // array_[0].second is the leftmost child.
  //
  // For keys:
  //   key < key[1]              -> value[0]
  //   key[i] <= key < key[i+1]  -> value[i]
  //   key >= key[size - 1]      -> value[size - 1]
  for (int i = 1; i < GetSize(); ++i) {
    if (comparator(key, array_[i].first)) {
      return array_[i - 1].second;
    }
  }

  return array_[GetSize() - 1].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &key,
                                                      const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = key;
  array_[1].second = new_value;
  SetSize(2);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &key,
                                                      const ValueType &new_value) -> int {
  int old_index = ValueIndex(old_value);

  if (old_index == -1) {
    return GetSize();
  }

  int insert_index = old_index + 1;

  for (int i = GetSize(); i > insert_index; --i) {
    array_[i] = array_[i - 1];
  }

  array_[insert_index].first = key;
  array_[insert_index].second = new_value;

  SetSize(GetSize() + 1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  if (index < 0 || index >= GetSize()) {
    return;
  }

  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }

  SetSize(GetSize() - 1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  ValueType only_child = array_[0].second;
  SetSize(0);
  return only_child;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  int recipient_size = recipient->GetSize();
  int size = GetSize();

  // Bring down the separator key from parent.
  recipient->array_[recipient_size].first = middle_key;
  recipient->array_[recipient_size].second = array_[0].second;

  // Copy valid keys from this node.
  for (int i = 1; i < size; ++i) {
    recipient->array_[recipient_size + i] = array_[i];
  }

  recipient->SetSize(recipient_size + size);
  SetSize(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  int size = GetSize();
  int middle_index = size / 2;

  // The middle key is pushed up to parent.
  // The right recipient starts with the child pointer to the right of middle_key.
  recipient->array_[0].first = middle_key;  // invalid in convention, but harmless
  recipient->array_[0].second = array_[middle_index].second;

  int recipient_index = 1;
  for (int i = middle_index + 1; i < size; ++i) {
    recipient->array_[recipient_index] = array_[i];
    recipient_index++;
  }

  recipient->SetSize(recipient_index);
  SetSize(middle_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  if (GetSize() == 0) {
    return;
  }

  int recipient_size = recipient->GetSize();

  recipient->array_[recipient_size].first = middle_key;
  recipient->array_[recipient_size].second = array_[0].second;
  recipient->SetSize(recipient_size + 1);

  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }

  SetSize(GetSize() - 1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key) {
  if (GetSize() == 0) {
    return;
  }

  int recipient_size = recipient->GetSize();

  for (int i = recipient_size; i > 0; --i) {
    recipient->array_[i] = recipient->array_[i - 1];
  }

  recipient->array_[0].first = middle_key;
  recipient->array_[0].second = array_[GetSize() - 1].second;
  recipient->SetSize(recipient_size + 1);

  SetSize(GetSize() - 1);
}

}  // namespace onebase