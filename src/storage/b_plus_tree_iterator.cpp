#include "onebase/storage/index/b_plus_tree_iterator.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTreeIterator<int, RID, std::less<int>>;

// ---------------------------------------------------------------------------
// Constructor (same as student stub)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
BPLUSTREE_ITERATOR_TYPE::BPlusTreeIterator(page_id_t page_id, int index)
    : page_id_(page_id), index_(index) {}

// ---------------------------------------------------------------------------
// IsEnd (same as student stub)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::IsEnd() const -> bool {
  return page_id_ == INVALID_PAGE_ID;
}

// ---------------------------------------------------------------------------
// operator*
//
// Design limitation: the iterator header only stores page_id_ and index_.
// To dereference, we would need a pointer to BufferPoolManager so we can
// FetchPage and read the leaf's array entry.  The current header does not
// store a BPM pointer.
//
// Students are expected to extend the header by adding a BPM* member and
// updating the constructor.  Until then, this method cannot be implemented
// correctly, so we throw NotImplementedException.
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator*() -> const std::pair<KeyType, ValueType> & {
  // Cannot implement: iterator lacks a BufferPoolManager pointer.
  // Students should add a BPM* field to the BPlusTreeIterator header and
  // use it here to FetchPage(page_id_), cast to LeafPage, and return
  // the pair at index_.
  throw NotImplementedException("BPlusTreeIterator::operator* -- iterator needs BPM access (extend header)");
}

// ---------------------------------------------------------------------------
// operator++
//
// Same design limitation as operator*.  Advancing requires reading the
// current leaf page to check if index_ has reached the end of that page,
// and if so, following next_page_id_ to the next leaf.
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator++() -> BPlusTreeIterator & {
  // Cannot implement: iterator lacks a BufferPoolManager pointer.
  // Students should add a BPM* field, increment index_, and if index_ >=
  // leaf->GetSize(), follow leaf->GetNextPageId() or set page_id_ to
  // INVALID_PAGE_ID if there is no next page.
  throw NotImplementedException("BPlusTreeIterator::operator++ -- iterator needs BPM access (extend header)");
}

// ---------------------------------------------------------------------------
// Comparison operators (same as student stub)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator==(const BPlusTreeIterator &other) const -> bool {
  return page_id_ == other.page_id_ && index_ == other.index_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_ITERATOR_TYPE::operator!=(const BPlusTreeIterator &other) const -> bool {
  return !(*this == other);
}

}  // namespace onebase
