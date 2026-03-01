#include "onebase/storage/page/b_plus_tree_internal_page.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeInternalPage<int, page_id_t, std::less<int>>;

// ---------------------------------------------------------------------------
// Provided helpers (same as student stub)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Reference implementations
// ---------------------------------------------------------------------------

/**
 * Linear scan over array_[0 .. size_) looking for the entry whose value
 * (child page pointer) equals the given value.  Returns the index, or -1
 * if not found.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/**
 * Find the child page that should contain the given key.
 *
 * Internal page layout:
 *   index 0: (unused_key, leftmost_child)
 *   index 1: (key_1, child_1)           -- child_1 has keys >= key_1
 *   index 2: (key_2, child_2)           -- child_2 has keys >= key_2
 *   ...
 *
 * We want the rightmost index i where key >= array_[i].first (for i >= 1).
 * If key < all keys, return array_[0].second (leftmost child).
 *
 * comparator(a, b) == true  means  a < b    (std::less semantics).
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                            const KeyComparator &comparator) const -> ValueType {
  // Scan from the last key down to index 1.  The first index i where
  // array_[i].first <= key (i.e. NOT (key < array_[i].first)) gives the
  // correct child pointer.
  for (int i = GetSize() - 1; i >= 1; i--) {
    if (!comparator(key, array_[i].first)) {
      // key >= array_[i].first
      return array_[i].second;
    }
  }
  // key < all keys => leftmost child
  return array_[0].second;
}

/**
 * Create a new root with one key and two children.
 * After this call the page has size == 2:
 *   array_[0].second = old_value  (left child)
 *   array_[1] = {key, new_value}  (right child)
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value,
                                                      const KeyType &key,
                                                      const ValueType &new_value) {
  array_[0].second = old_value;
  array_[1].first = key;
  array_[1].second = new_value;
  SetSize(2);
}

/**
 * Insert a new (key, new_value) pair immediately after the entry whose
 * value == old_value.  Returns the new size.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value,
                                                      const KeyType &key,
                                                      const ValueType &new_value) -> int {
  int idx = ValueIndex(old_value);
  // Shift entries [idx+1 .. size_) one position to the right.
  for (int i = GetSize(); i > idx + 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[idx + 1].first = key;
  array_[idx + 1].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

/**
 * Remove the entry at the given index by shifting everything after it left.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * Remove all entries and return the only remaining child pointer
 * (array_[0].second).  Sets size to 0.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType {
  ValueType child = array_[0].second;
  SetSize(0);
  return child;
}

/**
 * Move ALL entries from this page to the end of recipient.
 * middle_key is the key in the parent that separates recipient and this page;
 * it becomes the key for the first entry we push onto recipient.
 *
 * Before: recipient has entries [0..r_size), this has entries [0..my_size).
 * After:  recipient has entries [0..r_size + my_size), this is empty.
 *
 * The first transferred entry gets middle_key as its key (since our
 * array_[0].first is unused/invalid).
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient,
                                                const KeyType &middle_key) {
  int r_size = recipient->GetSize();

  // The key for entry [0] of this page is meaningless; use middle_key instead.
  recipient->array_[r_size].first = middle_key;
  recipient->array_[r_size].second = array_[0].second;

  for (int i = 1; i < GetSize(); i++) {
    recipient->array_[r_size + i] = array_[i];
  }

  recipient->IncreaseSize(GetSize());
  SetSize(0);
}

/**
 * Move the upper half of this page's entries to the (empty) recipient during
 * a split.
 *
 * The key at the split point is "pushed up" to the parent as middle_key
 * (it is NOT copied to recipient).
 *
 * Let split = GetSize() / 2.
 *   - Entry at index split: its key becomes the new middle_key for the parent.
 *     Its child pointer becomes recipient's array_[0].second.
 *   - Entries [split+1 .. size_) are copied verbatim to recipient.
 *
 * After: this page contains entries [0..split), recipient contains the rest.
 *
 * NOTE: The caller must read back the middle_key.  Since the parameter is
 *       const-ref in the header, we store it at recipient->array_[0].first
 *       so the caller can retrieve it via recipient->KeyAt(0) after this call.
 *       (An alternative design would use an out-parameter; here we match the
 *       header signature.)
 *
 * Wait -- re-reading the header, middle_key is passed IN (const ref), meaning
 * the caller already knows it.  For MoveHalfTo, the caller computes it as
 * KeyAt(split) before calling.  So we just set up the recipient correctly.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                  const KeyType &middle_key) {
  int split = GetSize() / 2;

  // The entry at `split` has the key that will be pushed to the parent
  // (middle_key).  Its child becomes the leftmost child of recipient.
  // Entries [split+1 .. size_) are the remaining children with their keys.

  // recipient array_[0].second = our array_[split].second  (leftmost child)
  // recipient array_[0].first  = middle_key (unused by convention but set for clarity)
  recipient->array_[0].second = array_[split].second;

  int j = 1;
  for (int i = split + 1; i < GetSize(); i++, j++) {
    recipient->array_[j] = array_[i];
  }

  recipient->SetSize(j);
  SetSize(split);
}

/**
 * Move the first entry of this page to the end of recipient.
 * middle_key (from the parent) becomes the key for the transferred entry.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient,
                                                       const KeyType &middle_key) {
  int r_size = recipient->GetSize();

  // Append to recipient: middle_key + our leftmost child pointer.
  recipient->array_[r_size].first = middle_key;
  recipient->array_[r_size].second = array_[0].second;
  recipient->IncreaseSize(1);

  // Shift our entries [1 .. size_) left to [0 .. size_-1).
  for (int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/**
 * Move the last entry of this page to the front of recipient.
 * middle_key (from the parent) becomes the key for the entry placed
 * at recipient index 1 (the current array_[0] of recipient stays as the
 * leftmost child).
 *
 * Actually, the convention is:
 *   - recipient's current array_[0].second is its leftmost child.
 *   - We shift everything in recipient right by 1.
 *   - Our last child becomes the new leftmost child (array_[0].second).
 *   - middle_key becomes array_[1].first (key to the left of the old leftmost).
 *
 * Wait, let me think again.  The parent key separating "this" (left) and
 * "recipient" (right) is middle_key.  We want to borrow the last child from
 * "this" and prepend it to "recipient".  After the redistribution, a new key
 * must go up to the parent (the caller handles that).
 *
 * Steps:
 *   1. Shift all entries of recipient right by 1.
 *   2. recipient->array_[0].second = our last child pointer.
 *   3. recipient->array_[1].first = middle_key.
 *      (This key separates the borrowed child from the rest of recipient.)
 *   4. Decrement our size, increment recipient size.
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient,
                                                        const KeyType &middle_key) {
  int r_size = recipient->GetSize();

  // Shift recipient entries right by 1.
  for (int i = r_size; i > 0; i--) {
    recipient->array_[i] = recipient->array_[i - 1];
  }

  // Place our last entry at front of recipient.
  int last = GetSize() - 1;
  recipient->array_[0].second = array_[last].second;
  recipient->array_[1].first = middle_key;

  recipient->IncreaseSize(1);
  IncreaseSize(-1);
}

}  // namespace onebase
