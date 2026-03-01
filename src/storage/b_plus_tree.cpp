#include "onebase/storage/index/b_plus_tree.h"
#include "onebase/storage/index/b_plus_tree_iterator.h"
#include <functional>
#include "onebase/common/exception.h"

namespace onebase {

template class BPlusTree<int, RID, std::less<int>>;

// ---------------------------------------------------------------------------
// Constructor (same as student stub)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// IsEmpty (same as student stub)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;
}

// ===========================================================================
//  PRIVATE HELPERS (defined as file-scope lambdas / inline in each method)
// ===========================================================================

// ---------------------------------------------------------------------------
//  Insert
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  // ------------------------------------------------------------------
  // Case 1: empty tree -- create a new leaf root.
  // ------------------------------------------------------------------
  if (IsEmpty()) {
    page_id_t new_page_id;
    Page *page = bpm_->NewPage(&new_page_id);
    if (page == nullptr) {
      return false;
    }
    auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());
    leaf->Init(leaf_max_size_);
    leaf->SetParentPageId(INVALID_PAGE_ID);
    leaf->Insert(key, value, comparator_);
    root_page_id_ = new_page_id;
    bpm_->UnpinPage(new_page_id, true);
    return true;
  }

  // ------------------------------------------------------------------
  // Case 2: tree is not empty -- find the correct leaf page.
  // ------------------------------------------------------------------

  // FindLeafPage: walk from root to leaf.
  auto find_leaf = [&](const KeyType &k) -> Page * {
    Page *page = bpm_->FetchPage(root_page_id_);
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    while (!tree_page->IsLeafPage()) {
      auto *internal = reinterpret_cast<InternalPage *>(tree_page);
      page_id_t child_id = internal->Lookup(k, comparator_);
      bpm_->UnpinPage(page->GetPageId(), false);
      page = bpm_->FetchPage(child_id);
      tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    return page;
  };

  Page *leaf_page = find_leaf(key);
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  int old_size = leaf->GetSize();
  int new_size = leaf->Insert(key, value, comparator_);

  // Duplicate key -- insert was a no-op.
  if (new_size == old_size) {
    bpm_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  // ------------------------------------------------------------------
  // No overflow.
  // ------------------------------------------------------------------
  if (new_size < leaf->GetMaxSize()) {
    bpm_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  // ------------------------------------------------------------------
  // Leaf overflow -- split.
  // ------------------------------------------------------------------
  page_id_t new_leaf_id;
  Page *new_leaf_page = bpm_->NewPage(&new_leaf_id);
  auto *new_leaf = reinterpret_cast<LeafPage *>(new_leaf_page->GetData());
  new_leaf->Init(leaf_max_size_);
  new_leaf->SetParentPageId(leaf->GetParentPageId());

  leaf->MoveHalfTo(new_leaf);

  // Fix the linked list: new_leaf already got leaf's old next pointer from
  // MoveHalfTo.  Now set leaf->next = new_leaf_id.
  leaf->SetNextPageId(new_leaf_id);

  // The key that goes up to the parent is the first key of the new leaf.
  KeyType middle_key = new_leaf->KeyAt(0);

  // ------------------------------------------------------------------
  // InsertIntoParent -- recursive helper implemented as a local
  // std::function so it has access to bpm_, comparator_, etc.
  // ------------------------------------------------------------------
  std::function<void(Page *, const KeyType &, Page *)> insert_into_parent;
  insert_into_parent = [&](Page *old_page, const KeyType &mid_key, Page *new_page) {
    auto *old_tree = reinterpret_cast<BPlusTreePage *>(old_page->GetData());
    auto *new_tree = reinterpret_cast<BPlusTreePage *>(new_page->GetData());

    // If old_page is the root, create a new root.
    if (old_tree->IsRootPage()) {
      page_id_t new_root_id;
      Page *new_root_page = bpm_->NewPage(&new_root_id);
      auto *new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
      new_root->Init(internal_max_size_);
      new_root->SetParentPageId(INVALID_PAGE_ID);
      new_root->PopulateNewRoot(old_page->GetPageId(), mid_key, new_page->GetPageId());

      old_tree->SetParentPageId(new_root_id);
      new_tree->SetParentPageId(new_root_id);

      root_page_id_ = new_root_id;

      bpm_->UnpinPage(old_page->GetPageId(), true);
      bpm_->UnpinPage(new_page->GetPageId(), true);
      bpm_->UnpinPage(new_root_id, true);
      return;
    }

    // Otherwise, insert into the existing parent.
    page_id_t parent_id = old_tree->GetParentPageId();
    Page *parent_page = bpm_->FetchPage(parent_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

    new_tree->SetParentPageId(parent_id);
    parent->InsertNodeAfter(old_page->GetPageId(), mid_key, new_page->GetPageId());

    bpm_->UnpinPage(old_page->GetPageId(), true);
    bpm_->UnpinPage(new_page->GetPageId(), true);

    // Check if the parent overflows.
    if (parent->GetSize() < parent->GetMaxSize()) {
      bpm_->UnpinPage(parent_id, true);
      return;
    }

    // Parent overflow -- split the internal page.
    // The key at split point goes up to grandparent.
    KeyType parent_mid_key = parent->KeyAt(parent->GetSize() / 2);

    page_id_t new_internal_id;
    Page *new_internal_page = bpm_->NewPage(&new_internal_id);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_internal_page->GetData());
    new_internal->Init(internal_max_size_);
    new_internal->SetParentPageId(parent->GetParentPageId());

    parent->MoveHalfTo(new_internal, parent_mid_key);

    // Update children of the new internal page to point to it as parent.
    for (int i = 0; i < new_internal->GetSize(); i++) {
      page_id_t child_id = new_internal->ValueAt(i);
      Page *child_page = bpm_->FetchPage(child_id);
      auto *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child->SetParentPageId(new_internal_id);
      bpm_->UnpinPage(child_id, true);
    }

    // Recursively insert the parent_mid_key into grandparent.
    insert_into_parent(parent_page, parent_mid_key, new_internal_page);
  };

  insert_into_parent(leaf_page, middle_key, new_leaf_page);
  return true;
}

// ---------------------------------------------------------------------------
//  Remove
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  if (IsEmpty()) {
    return;
  }

  // FindLeafPage.
  auto find_leaf = [&](const KeyType &k) -> Page * {
    Page *page = bpm_->FetchPage(root_page_id_);
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    while (!tree_page->IsLeafPage()) {
      auto *internal = reinterpret_cast<InternalPage *>(tree_page);
      page_id_t child_id = internal->Lookup(k, comparator_);
      bpm_->UnpinPage(page->GetPageId(), false);
      page = bpm_->FetchPage(child_id);
      tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    return page;
  };

  Page *leaf_page = find_leaf(key);
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  int old_size = leaf->GetSize();
  int new_size = leaf->RemoveAndDeleteRecord(key, comparator_);

  // Key not found.
  if (new_size == old_size) {
    bpm_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }

  // ------------------------------------------------------------------
  // If the leaf is the root:
  // ------------------------------------------------------------------
  if (leaf->IsRootPage()) {
    if (new_size == 0) {
      // Tree is now empty.
      bpm_->UnpinPage(leaf_page->GetPageId(), true);
      bpm_->DeletePage(leaf_page->GetPageId());
      root_page_id_ = INVALID_PAGE_ID;
    } else {
      bpm_->UnpinPage(leaf_page->GetPageId(), true);
    }
    return;
  }

  // ------------------------------------------------------------------
  // Leaf is not root.  Check for underflow.
  // ------------------------------------------------------------------
  if (new_size >= leaf->GetMinSize()) {
    bpm_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }

  // ------------------------------------------------------------------
  // CoalesceOrRedistribute -- recursive helper.
  // ------------------------------------------------------------------
  // Forward declaration as std::function to allow recursion.
  std::function<void(Page *)> coalesce_or_redistribute;

  coalesce_or_redistribute = [&](Page *page) {
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

    if (tree_page->IsRootPage()) {
      // Root underflow: if root is internal with size == 1, it has only one
      // child pointer.  Promote that child as the new root.
      if (!tree_page->IsLeafPage() && tree_page->GetSize() == 1) {
        auto *root = reinterpret_cast<InternalPage *>(tree_page);
        page_id_t child_id = root->RemoveAndReturnOnlyChild();

        Page *child_page = bpm_->FetchPage(child_id);
        auto *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        child->SetParentPageId(INVALID_PAGE_ID);
        bpm_->UnpinPage(child_id, true);

        root_page_id_ = child_id;
        bpm_->UnpinPage(page->GetPageId(), true);
        bpm_->DeletePage(page->GetPageId());
      } else {
        bpm_->UnpinPage(page->GetPageId(), true);
      }
      return;
    }

    // Fetch parent.
    page_id_t parent_id = tree_page->GetParentPageId();
    Page *parent_page = bpm_->FetchPage(parent_id);
    auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

    // Find this node's index in parent.
    int node_index = parent->ValueIndex(page->GetPageId());

    // Choose sibling: prefer left sibling if it exists; otherwise right.
    bool sibling_is_left = (node_index > 0);
    int sibling_index = sibling_is_left ? node_index - 1 : node_index + 1;
    page_id_t sibling_id = parent->ValueAt(sibling_index);
    Page *sibling_page = bpm_->FetchPage(sibling_id);
    auto *sibling_tree = reinterpret_cast<BPlusTreePage *>(sibling_page->GetData());

    // The key in the parent between sibling and node.
    int parent_key_index = sibling_is_left ? node_index : sibling_index;
    KeyType parent_key = parent->KeyAt(parent_key_index);

    // ------------------------------------------------------------------
    // Can we merge?
    // ------------------------------------------------------------------
    if (tree_page->IsLeafPage()) {
      auto *node_leaf = reinterpret_cast<LeafPage *>(tree_page);
      auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_tree);

      if (sibling_leaf->GetSize() + node_leaf->GetSize() < node_leaf->GetMaxSize()) {
        // Merge: always merge the right node into the left node.
        LeafPage *left;
        LeafPage *right;
        Page *left_page;
        Page *right_page;
        if (sibling_is_left) {
          left = sibling_leaf;
          left_page = sibling_page;
          right = node_leaf;
          right_page = page;
        } else {
          left = node_leaf;
          left_page = page;
          right = sibling_leaf;
          right_page = sibling_page;
        }

        right->MoveAllTo(left);

        // Remove entry for the right node from parent.
        int remove_index = parent->ValueIndex(right_page->GetPageId());
        parent->Remove(remove_index);

        bpm_->UnpinPage(left_page->GetPageId(), true);
        bpm_->UnpinPage(right_page->GetPageId(), true);
        bpm_->DeletePage(right_page->GetPageId());

        // Recursively check parent for underflow.
        if (parent->GetSize() < parent->GetMinSize()) {
          coalesce_or_redistribute(parent_page);
        } else {
          bpm_->UnpinPage(parent_id, true);
        }
      } else {
        // Redistribute.
        if (sibling_is_left) {
          // Borrow last from left sibling.
          sibling_leaf->MoveLastToFrontOf(node_leaf);
          // Update parent key.
          parent->SetKeyAt(parent_key_index, node_leaf->KeyAt(0));
        } else {
          // Borrow first from right sibling.
          sibling_leaf->MoveFirstToEndOf(node_leaf);
          // Update parent key.
          parent->SetKeyAt(parent_key_index, sibling_leaf->KeyAt(0));
        }
        bpm_->UnpinPage(page->GetPageId(), true);
        bpm_->UnpinPage(sibling_id, true);
        bpm_->UnpinPage(parent_id, true);
      }
    } else {
      // Internal page.
      auto *node_internal = reinterpret_cast<InternalPage *>(tree_page);
      auto *sibling_internal = reinterpret_cast<InternalPage *>(sibling_tree);

      if (sibling_internal->GetSize() + node_internal->GetSize() <= node_internal->GetMaxSize()) {
        // Merge: merge right into left.
        InternalPage *left;
        InternalPage *right;
        Page *left_page;
        Page *right_page;
        if (sibling_is_left) {
          left = sibling_internal;
          left_page = sibling_page;
          right = node_internal;
          right_page = page;
        } else {
          left = node_internal;
          left_page = page;
          right = sibling_internal;
          right_page = sibling_page;
        }

        right->MoveAllTo(left, parent_key);

        // Update parent pointers for transferred children.
        for (int i = left->GetSize() - right->GetSize(); i < left->GetSize(); i++) {
          // Actually, after MoveAllTo, right's size is 0 and left has all.
          // We need to iterate over the newly added children.
        }
        // The children moved from right to left need their parent updated.
        // After MoveAllTo: left has old left entries + middle_key + old right entries.
        // The old right entries start at (old_left_size).
        // But right's size is now 0, so let's just update all children of left
        // that were from right.  Simpler: iterate from old_left_size to left->GetSize().
        // We don't have old_left_size easily, so update all of left's children.
        for (int i = 0; i < left->GetSize(); i++) {
          page_id_t child_id = left->ValueAt(i);
          Page *child_page = bpm_->FetchPage(child_id);
          auto *child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
          child->SetParentPageId(left_page->GetPageId());
          bpm_->UnpinPage(child_id, true);
        }

        // Remove entry for right from parent.
        int remove_index = parent->ValueIndex(right_page->GetPageId());
        parent->Remove(remove_index);

        bpm_->UnpinPage(left_page->GetPageId(), true);
        bpm_->UnpinPage(right_page->GetPageId(), true);
        bpm_->DeletePage(right_page->GetPageId());

        // Recursively check parent.
        if (parent->GetSize() < parent->GetMinSize()) {
          coalesce_or_redistribute(parent_page);
        } else {
          bpm_->UnpinPage(parent_id, true);
        }
      } else {
        // Redistribute internal pages.
        if (sibling_is_left) {
          // Borrow last from left sibling.
          // The parent key goes down to this node; sibling's last key goes up.
          KeyType new_parent_key = sibling_internal->KeyAt(sibling_internal->GetSize() - 1);
          sibling_internal->MoveLastToFrontOf(node_internal, parent_key);

          // Update the child's parent pointer.
          page_id_t moved_child_id = node_internal->ValueAt(0);
          Page *moved_child_page = bpm_->FetchPage(moved_child_id);
          auto *moved_child = reinterpret_cast<BPlusTreePage *>(moved_child_page->GetData());
          moved_child->SetParentPageId(page->GetPageId());
          bpm_->UnpinPage(moved_child_id, true);

          parent->SetKeyAt(parent_key_index, new_parent_key);
        } else {
          // Borrow first from right sibling.
          KeyType new_parent_key = sibling_internal->KeyAt(1);
          sibling_internal->MoveFirstToEndOf(node_internal, parent_key);

          // Update the child's parent pointer.
          page_id_t moved_child_id = node_internal->ValueAt(node_internal->GetSize() - 1);
          Page *moved_child_page = bpm_->FetchPage(moved_child_id);
          auto *moved_child = reinterpret_cast<BPlusTreePage *>(moved_child_page->GetData());
          moved_child->SetParentPageId(page->GetPageId());
          bpm_->UnpinPage(moved_child_id, true);

          parent->SetKeyAt(parent_key_index, new_parent_key);
        }
        bpm_->UnpinPage(page->GetPageId(), true);
        bpm_->UnpinPage(sibling_id, true);
        bpm_->UnpinPage(parent_id, true);
      }
    }
  };

  coalesce_or_redistribute(leaf_page);
}

// ---------------------------------------------------------------------------
//  GetValue
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  if (IsEmpty()) {
    return false;
  }

  // FindLeafPage.
  Page *page = bpm_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!tree_page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(tree_page);
    page_id_t child_id = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(page->GetPageId(), false);
    page = bpm_->FetchPage(child_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(tree_page);
  ValueType val;
  bool found = leaf->Lookup(key, &val, comparator_);
  bpm_->UnpinPage(page->GetPageId(), false);

  if (found) {
    result->push_back(val);
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
//  Iterator: Begin()
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin() -> Iterator {
  if (IsEmpty()) {
    return Iterator(INVALID_PAGE_ID, 0);
  }

  // Traverse to the leftmost leaf: always follow index-0 child.
  Page *page = bpm_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!tree_page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(tree_page);
    page_id_t child_id = internal->ValueAt(0);
    bpm_->UnpinPage(page->GetPageId(), false);
    page = bpm_->FetchPage(child_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  page_id_t leaf_id = page->GetPageId();
  bpm_->UnpinPage(leaf_id, false);
  return Iterator(leaf_id, 0);
}

// ---------------------------------------------------------------------------
//  Iterator: Begin(key)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> Iterator {
  if (IsEmpty()) {
    return Iterator(INVALID_PAGE_ID, 0);
  }

  // FindLeafPage.
  Page *page = bpm_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!tree_page->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(tree_page);
    page_id_t child_id = internal->Lookup(key, comparator_);
    bpm_->UnpinPage(page->GetPageId(), false);
    page = bpm_->FetchPage(child_id);
    tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  auto *leaf = reinterpret_cast<LeafPage *>(tree_page);
  int index = leaf->KeyIndex(key, comparator_);
  page_id_t leaf_id = page->GetPageId();
  bpm_->UnpinPage(leaf_id, false);
  return Iterator(leaf_id, index);
}

// ---------------------------------------------------------------------------
//  Iterator: End() (same as student stub)
// ---------------------------------------------------------------------------

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPLUSTREE_TYPE::End() -> Iterator {
  return Iterator(INVALID_PAGE_ID, 0);
}

}  // namespace onebase
