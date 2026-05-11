#include "onebase/storage/page/b_plus_tree_leaf_page.h"

#include <functional>

#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeLeafPage<int, RID, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  next_page_id_ = INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 0;
  int right = GetSize();

  while (left < right) {
    int mid = left + (right - left) / 2;

    if (comparator(array_[mid].first, key)) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value,
                                         const KeyComparator &comparator) const -> bool {
  int index = KeyIndex(key, comparator);

  if (index >= GetSize()) {
    return false;
  }

  const KeyType &candidate = array_[index].first;

  if (!comparator(candidate, key) && !comparator(key, candidate)) {
    *value = array_[index].second;
    return true;
  }

  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                         const KeyComparator &comparator) -> int {
  int index = KeyIndex(key, comparator);

  if (index < GetSize()) {
    const KeyType &candidate = array_[index].first;
    if (!comparator(candidate, key) && !comparator(key, candidate)) {
      return GetSize();
    }
  }

  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }

  array_[index] = std::make_pair(key, value);
  SetSize(GetSize() + 1);

  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key,
                                                        const KeyComparator &comparator) -> int {
  int index = KeyIndex(key, comparator);

  if (index >= GetSize()) {
    return GetSize();
  }

  const KeyType &candidate = array_[index].first;
  if (comparator(candidate, key) || comparator(key, candidate)) {
    return GetSize();
  }

  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }

  SetSize(GetSize() - 1);
  return GetSize();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int total_size = GetSize();
  int start = total_size / 2;
  int move_count = total_size - start;

  for (int i = 0; i < move_count; ++i) {
    recipient->array_[i] = array_[start + i];
  }

  recipient->SetSize(move_count);
  SetSize(start);

  recipient->SetNextPageId(GetNextPageId());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  int recipient_size = recipient->GetSize();
  int size = GetSize();

  for (int i = 0; i < size; ++i) {
    recipient->array_[recipient_size + i] = array_[i];
  }

  recipient->SetSize(recipient_size + size);
  SetSize(0);

  recipient->SetNextPageId(GetNextPageId());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  if (GetSize() == 0) {
    return;
  }

  int recipient_size = recipient->GetSize();

  recipient->array_[recipient_size] = array_[0];
  recipient->SetSize(recipient_size + 1);

  for (int i = 0; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }

  SetSize(GetSize() - 1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  if (GetSize() == 0) {
    return;
  }

  int recipient_size = recipient->GetSize();

  for (int i = recipient_size; i > 0; --i) {
    recipient->array_[i] = recipient->array_[i - 1];
  }

  recipient->array_[0] = array_[GetSize() - 1];
  recipient->SetSize(recipient_size + 1);

  SetSize(GetSize() - 1);
}

}  // namespace onebase