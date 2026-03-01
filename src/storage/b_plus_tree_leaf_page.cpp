#include "onebase/storage/page/b_plus_tree_leaf_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeLeafPage<int, RID, std::less<int>>;

// ---------------------------------------------------------------------------
// Provided helpers (same as student stub)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Reference implementations
// ---------------------------------------------------------------------------

/**
 * Find the first index i in [0, size_) where array_[i].first >= key.
 * If no such index exists, returns GetSize() (one past the end).
 *
 * This is the insert position for the given key.
 * A linear scan suffices for the reference; students may upgrade to binary
 * search as a bonus.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key,
                                           const KeyComparator &comparator) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    // comparator(a, b) == true  means  a < b.
    // We want the first i where key <= array_[i].first,
    // i.e. NOT (array_[i].first < key).
    if (!comparator(array_[i].first, key)) {
      return i;
    }
  }
  return GetSize();
}

/**
 * Look up a key.  If found, set *value and return true; otherwise false.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value,
                                         const KeyComparator &comparator) const -> bool {
  int pos = KeyIndex(key, comparator);
  // Check that pos is in range and the key matches exactly.
  // Exact match: neither key < array_[pos].first  nor  array_[pos].first < key.
  if (pos < GetSize() && !comparator(key, array_[pos].first) && !comparator(array_[pos].first, key)) {
    *value = array_[pos].second;
    return true;
  }
  return false;
}

/**
 * Insert a (key, value) pair in sorted order.
 * If the key already exists, do nothing (no duplicates).
 * Returns the new size (unchanged if duplicate).
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                         const KeyComparator &comparator) -> int {
  int pos = KeyIndex(key, comparator);

  // Check for duplicate: if array_[pos].first == key, return current size.
  if (pos < GetSize() && !comparator(key, array_[pos].first) && !comparator(array_[pos].first, key)) {
    return GetSize();
  }

  // Shift entries [pos, size_) right by 1.
  for (int i = GetSize(); i > pos; i--) {
    array_[i] = array_[i - 1];
  }

  array_[pos].first = key;
  array_[pos].second = value;
  IncreaseSize(1);
  return GetSize();
}

/**
 * Remove the entry with the given key.
 * Returns the new size (unchanged if key not found).
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key,
                                                        const KeyComparator &comparator) -> int {
  int pos = KeyIndex(key, comparator);

  // Key not found if pos is out of range or key mismatch.
  if (pos >= GetSize() || comparator(key, array_[pos].first) || comparator(array_[pos].first, key)) {
    return GetSize();
  }

  // Shift entries [pos+1, size_) left by 1.
  for (int i = pos; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
}

/**
 * Move the upper half [size_/2, size_) of entries to the (empty) recipient
 * during a leaf split.
 *
 * Also update the linked-list pointers:
 *   recipient->next_page_id_ = this->next_page_id_
 *   (The caller is responsible for setting this->next_page_id_ to recipient's
 *    page id, since the leaf page doesn't know its own page_id.)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int split = GetSize() / 2;
  int count = GetSize() - split;

  for (int i = 0; i < count; i++) {
    recipient->array_[i] = array_[split + i];
  }

  // Linked list: recipient takes over our next pointer.
  recipient->SetNextPageId(next_page_id_);

  recipient->SetSize(count);
  SetSize(split);
}

/**
 * Move all entries to the end of recipient during a merge.
 * Update the linked-list pointer: recipient->next = this->next.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  int r_size = recipient->GetSize();

  for (int i = 0; i < GetSize(); i++) {
    recipient->array_[r_size + i] = array_[i];
  }

  recipient->SetNextPageId(next_page_id_);
  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

/**
 * Move this page's first entry to the end of recipient (redistribute right
 * to left).  Shift remaining entries left.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  int r_size = recipient->GetSize();
  recipient->array_[r_size] = array_[0];
  recipient->IncreaseSize(1);

  // Shift our entries left.
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * Move this page's last entry to the front of recipient (redistribute left
 * to right).  Shift recipient's entries right first.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  int r_size = recipient->GetSize();

  // Shift recipient right by 1.
  for (int i = r_size; i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }

  recipient->array_[0] = array_[GetSize() - 1];
  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

}  // namespace onebase
