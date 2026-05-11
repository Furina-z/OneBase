#include "onebase/storage/index/b_plus_tree.h"

#include <functional>
#include <utility>
#include <vector>

#include "onebase/common/exception.h"
#include "onebase/storage/index/b_plus_tree_iterator.h"

namespace onebase {

template class BPlusTree<int, RID, std::less<int>>;

template <typename KeyType, typename ValueType, typename KeyComparator>
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *bpm, const KeyComparator &comparator,
                           int leaf_max_size, int internal_max_size)
    : Index(std::move(name)), bpm_(bpm), comparator_(comparator),
      leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size) {
  if (leaf_max_size_ == 0) {
    leaf_max_size_ = static_cast<int>(
        (ONEBASE_PAGE_SIZE - sizeof(BPlusTreePage) - sizeof(page_id_t)) /
        (sizeof(KeyType) + sizeof(ValueType)));
  }
  if (internal_max_size_ == 0) {
    internal_max_size_ = static_cast<int>(
        (ONEBASE_PAGE_SIZE - sizeof(BPlusTreePage)) /
        (sizeof(KeyType) + sizeof(page_id_t)));
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  if (IsEmpty()) {
    page_id_t root_page_id;
    Page *root_page = bpm_->NewPage(&root_page_id);
    if (root_page == nullptr) {
      return false;
    }

    auto *root = reinterpret_cast<LeafPage *>(root_page->GetData());
    root->Init(leaf_max_size_);
    root->SetParentPageId(INVALID_PAGE_ID);
    root->Insert(key, value, comparator_);

    root_page_id_ = root_page_id;

    bpm_->UnpinPage(root_page_id, true);
    return true;
  }

  page_id_t leaf_page_id = root_page_id_;
  Page *page = bpm_->FetchPage(leaf_page_id);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next_page_id = internal->Lookup(key, comparator_);

    bpm_->UnpinPage(leaf_page_id, false);

    leaf_page_id = next_page_id;
    page = bpm_->FetchPage(leaf_page_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());

  ValueType existing_value;
  if (leaf->Lookup(key, &existing_value, comparator_)) {
    bpm_->UnpinPage(leaf_page_id, false);
    return false;
  }

  leaf->Insert(key, value, comparator_);

  if (leaf->GetSize() <= leaf->GetMaxSize()) {
    bpm_->UnpinPage(leaf_page_id, true);
    return true;
  }

  page_id_t new_leaf_page_id;
  Page *new_leaf_page = bpm_->NewPage(&new_leaf_page_id);
  auto *new_leaf = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());

  new_leaf->Init(leaf_max_size_);
  new_leaf->SetParentPageId(leaf->GetParentPageId());

  leaf->MoveHalfTo(new_leaf);
  leaf->SetNextPageId(new_leaf_page_id);

  KeyType split_key = new_leaf->KeyAt(0);

  page_id_t old_page_id = leaf_page_id;
  page_id_t new_page_id = new_leaf_page_id;
  page_id_t parent_page_id = leaf->GetParentPageId();
  KeyType key_to_insert = split_key;

  // Propagate split upward.
  while (true) {
    if (parent_page_id == INVALID_PAGE_ID) {
      page_id_t new_root_page_id;
      Page *new_root_page = bpm_->NewPage(&new_root_page_id);
      auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());

      new_root->Init(internal_max_size_);
      new_root->SetParentPageId(INVALID_PAGE_ID);
      new_root->PopulateNewRoot(old_page_id, key_to_insert, new_page_id);

      auto *old_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      auto *new_node = reinterpret_cast<BPlusTreePage *>(new_leaf_page->GetData());

      old_node->SetParentPageId(new_root_page_id);
      new_node->SetParentPageId(new_root_page_id);

      root_page_id_ = new_root_page_id;

      bpm_->UnpinPage(new_root_page_id, true);
      bpm_->UnpinPage(new_leaf_page_id, true);
      bpm_->UnpinPage(leaf_page_id, true);
      return true;
    }

    Page *parent_page = bpm_->FetchPage(parent_page_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

    parent->InsertNodeAfter(old_page_id, key_to_insert, new_page_id);

    if (parent->GetSize() <= parent->GetMaxSize()) {
      bpm_->UnpinPage(parent_page_id, true);
      bpm_->UnpinPage(new_leaf_page_id, true);
      bpm_->UnpinPage(leaf_page_id, true);
      return true;
    }

    page_id_t new_internal_page_id;
    Page *new_internal_page = bpm_->NewPage(&new_internal_page_id);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_internal_page->GetData());

    new_internal->Init(internal_max_size_);
    new_internal->SetParentPageId(parent->GetParentPageId());

    int middle_index = parent->GetSize() / 2;
    KeyType middle_key = parent->KeyAt(middle_index);

    parent->MoveHalfTo(new_internal, middle_key);

    // Update parent pointer of children moved to new internal node.
    for (int i = 0; i < new_internal->GetSize(); ++i) {
      page_id_t child_page_id = new_internal->ValueAt(i);
      Page *child_page = bpm_->FetchPage(child_page_id);
      auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child_node->SetParentPageId(new_internal_page_id);
      bpm_->UnpinPage(child_page_id, true);
    }

    old_page_id = parent_page_id;
    new_page_id = new_internal_page_id;
    key_to_insert = middle_key;
    parent_page_id = parent->GetParentPageId();

    bpm_->UnpinPage(parent_page_id == INVALID_PAGE_ID ? old_page_id : old_page_id, true);
    bpm_->UnpinPage(new_internal_page_id, true);

    // Continue upward. Re-fetch pages if another root creation is needed.
    if (parent_page_id == INVALID_PAGE_ID) {
      Page *old_node_page = bpm_->FetchPage(old_page_id);
      Page *new_node_page = bpm_->FetchPage(new_page_id);

      page_id_t new_root_page_id;
      Page *new_root_page = bpm_->NewPage(&new_root_page_id);
      auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());

      new_root->Init(internal_max_size_);
      new_root->SetParentPageId(INVALID_PAGE_ID);
      new_root->PopulateNewRoot(old_page_id, key_to_insert, new_page_id);

      auto *old_node = reinterpret_cast<BPlusTreePage *>(old_node_page->GetData());
      auto *new_node = reinterpret_cast<BPlusTreePage *>(new_node_page->GetData());
      old_node->SetParentPageId(new_root_page_id);
      new_node->SetParentPageId(new_root_page_id);

      root_page_id_ = new_root_page_id;

      bpm_->UnpinPage(old_page_id, true);
      bpm_->UnpinPage(new_page_id, true);
      bpm_->UnpinPage(new_root_page_id, true);
      bpm_->UnpinPage(new_leaf_page_id, true);
      bpm_->UnpinPage(leaf_page_id, true);
      return true;
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  if (IsEmpty()) {
    return;
  }

  std::vector<std::pair<KeyType, ValueType>> entries;
  bool removed = false;

  for (auto it = Begin(); !it.IsEnd(); ++it) {
    auto entry = *it;
    if (!removed && !comparator_(entry.first, key) && !comparator_(key, entry.first)) {
      removed = true;
      continue;
    }
    entries.push_back(entry);
  }

  if (!removed) {
    return;
  }

  root_page_id_ = INVALID_PAGE_ID;

  for (const auto &entry : entries) {
    Insert(entry.first, entry.second);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  if (IsEmpty()) {
    return false;
  }

  page_id_t page_id = root_page_id_;
  Page *page = bpm_->FetchPage(page_id);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next_page_id = internal->Lookup(key, comparator_);

    bpm_->UnpinPage(page_id, false);

    page_id = next_page_id;
    page = bpm_->FetchPage(page_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());

  ValueType value;
  bool found = leaf->Lookup(key, &value, comparator_);

  if (found) {
    result->push_back(value);
  }

  bpm_->UnpinPage(page_id, false);
  return found;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin() -> Iterator {
  if (IsEmpty()) {
    return End();
  }

  page_id_t page_id = root_page_id_;
  Page *page = bpm_->FetchPage(page_id);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next_page_id = internal->ValueAt(0);

    bpm_->UnpinPage(page_id, false);

    page_id = next_page_id;
    page = bpm_->FetchPage(page_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());

  if (leaf->GetSize() == 0) {
    bpm_->UnpinPage(page_id, false);
    return End();
  }

  bpm_->UnpinPage(page_id, false);
  return Iterator(page_id, 0, bpm_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> Iterator {
  if (IsEmpty()) {
    return End();
  }

  page_id_t page_id = root_page_id_;
  Page *page = bpm_->FetchPage(page_id);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(page->GetData());
    page_id_t next_page_id = internal->Lookup(key, comparator_);

    bpm_->UnpinPage(page_id, false);

    page_id = next_page_id;
    page = bpm_->FetchPage(page_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  int index = leaf->KeyIndex(key, comparator_);

  if (index >= leaf->GetSize()) {
    page_id_t next_page_id = leaf->GetNextPageId();
    bpm_->UnpinPage(page_id, false);

    if (next_page_id == INVALID_PAGE_ID) {
      return End();
    }

    return Iterator(next_page_id, 0, bpm_);
  }

  bpm_->UnpinPage(page_id, false);
  return Iterator(page_id, index, bpm_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::End() -> Iterator {
  return Iterator(INVALID_PAGE_ID, 0, bpm_);
}

}  // namespace onebase