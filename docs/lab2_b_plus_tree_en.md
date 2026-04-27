# Lab 2: B+ Tree Index

## 1. Overview

The goal of this lab is to implement a complete **B+ Tree Index** for the OneBase database. The B+ Tree is the most widely used index structure in relational databases -- it supports efficient point queries and range queries, with O(log n) time complexity for search, insertion, and deletion.

You will implement the following four components:

1. **Internal Page** (`src/storage/page/b_plus_tree_internal_page.cpp`) -- B+ Tree internal node page
2. **Leaf Page** (`src/storage/page/b_plus_tree_leaf_page.cpp`) -- B+ Tree leaf node page
3. **B+ Tree** (`src/storage/index/b_plus_tree.cpp`) -- Core B+ Tree operations (search, insert, delete)
4. **B+ Tree Iterator** (`src/storage/index/b_plus_tree_iterator.cpp`) -- Sequential traversal of leaf nodes

## 2. Background

### 2.1 B+ Tree Structure

```
                    ┌─────────────┐
                    │  Internal   │
                    │  [_, 5, 10] │        key[0] is invalid
                    │ p0  p1  p2  │        value[i] is child page_id
                    └──┬────┬────┬┘
                       │    │    │
           ┌───────────┘    │    └───────────┐
           ▼                ▼                ▼
    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
    │   Leaf       │ │   Leaf       │ │   Leaf       │
    │ [1,2,3,4]   │→│ [5,6,7,8,9] │→│ [10,11,12]  │
    │  v v v v     │ │  v v v v v  │ │  v  v  v    │
    └──────────────┘ └──────────────┘ └──────────────┘
         next_page_id →    next_page_id →    INVALID
```

**Key Properties:**
- All data is stored in leaf nodes
- Internal nodes only store routing keys + child pointers
- Leaf nodes are linked via `next_page_id_` forming a singly linked list
- The `array_[0].first` (first key) of an internal node is invalid; only `array_[0].second` (pointer) is valid

### 2.2 Page Layout

Each B+ Tree node is stored in a database page. The memory layout of a page is as follows:

```
┌─────────────────── Page (4096 bytes) ────────────────────┐
│                                                          │
│  BPlusTreePage header:                                   │
│    page_type_     (IndexPageType: LEAF or INTERNAL)      │
│    size_          (current number of key-value pairs)     │
│    max_size_      (maximum capacity)                      │
│    parent_page_id_ (parent node page_id)                  │
│                                                          │
│  [Leaf only] next_page_id_  (next leaf node)              │
│                                                          │
│  array_[0]:  { key_0, value_0 }                          │
│  array_[1]:  { key_1, value_1 }                          │
│  ...                                                     │
│  array_[n-1]: { key_{n-1}, value_{n-1} }                 │
└──────────────────────────────────────────────────────────┘
```

**Important Details:**
- `array_[0]` uses the C flexible array member trick (declared as `std::pair<KeyType, ValueType> array_[0]`)
- The actual usable array size is determined by `max_size_`, using `reinterpret_cast` to treat the Page's `data_` region as the node
- For internal nodes: `ValueType` = `page_id_t` (child page ID)
- For leaf nodes: `ValueType` = `RID` (record identifier)

### 2.3 Split and Merge

**Split:** Triggered when `size > max_size` after insertion into a node

```
Before split (leaf, max_size=4):
  [1, 2, 3, 4, 5]     <- size=5 > max_size=4, split required

After split:
  Original node: [1, 2]       <- keep the first half
  New node:      [3, 4, 5]    <- second half moves to new node
  Push up: key=3 to parent node
```

**Merge:** Triggered when `size < min_size` after deletion from a node
- `min_size` = `max_size / 2` (leaf nodes) or `(max_size + 1) / 2` (internal nodes)
- First attempt to **Redistribute** from a sibling node
- If the sibling is also not rich enough, **Merge** the two nodes

### 2.4 KeyComparator

The B+ Tree is a template class `BPlusTree<KeyType, ValueType, KeyComparator>`. The default instantiation in this project is:
- `KeyType` = `int`
- `ValueType` = `RID`
- `KeyComparator` = `std::less<int>`

`comparator(a, b)` returns `true` meaning `a < b`.

## 3. Your Tasks

### Task 1: Internal Page Methods

**File:** `src/storage/page/b_plus_tree_internal_page.cpp`

| Method | Description | Difficulty |
|--------|-------------|------------|
| `Init(max_size)` | Initialize page metadata | ★☆☆ |
| `KeyAt(index)` / `SetKeyAt(index, key)` | Read/write the key at a given position | ★☆☆ |
| `ValueAt(index)` / `SetValueAt(index, value)` | Read/write the value at a given position | ★☆☆ |
| `ValueIndex(value)` | Find the index of a given value (child page_id) | ★☆☆ |
| `Lookup(key, comparator)` | Find the child page_id that the key should route to | ★★☆ |
| `PopulateNewRoot(old_val, key, new_val)` | Initialize a new root node | ★☆☆ |
| `InsertNodeAfter(old_val, key, new_val)` | Insert a new key-value after old_val | ★★☆ |
| `Remove(index)` | Remove the key-value at a given position | ★☆☆ |
| `RemoveAndReturnOnlyChild()` | Remove the root node and return its only child | ★☆☆ |
| `MoveAllTo(recipient, middle_key)` | Move all elements to recipient | ★★☆ |
| `MoveHalfTo(recipient, middle_key)` | Move the second half to recipient (for split) | ★★☆ |
| `MoveFirstToEndOf(recipient, middle_key)` | Move the first element to the end of recipient | ★★☆ |
| `MoveLastToFrontOf(recipient, middle_key)` | Move the last element to the front of recipient | ★★☆ |

### Task 2: Leaf Page Methods

**File:** `src/storage/page/b_plus_tree_leaf_page.cpp`

| Method | Description | Difficulty |
|--------|-------------|------------|
| `Init(max_size)` | Initialize page metadata | ★☆☆ |
| `KeyAt(index)` / `ValueAt(index)` | Read the key/value at a given position | ★☆☆ |
| `KeyIndex(key, comparator)` | Binary search for the first position >= key | ★★☆ |
| `Lookup(key, value, comparator)` | Exact lookup for the value corresponding to a key | ★★☆ |
| `Insert(key, value, comparator)` | Insert key-value at the sorted position | ★★★ |
| `RemoveAndDeleteRecord(key, comparator)` | Remove the specified key | ★★☆ |
| `MoveHalfTo(recipient)` | Move the second half to recipient (for split) | ★★☆ |
| `MoveAllTo(recipient)` | Move all elements to recipient (for merge) | ★★☆ |
| `MoveFirstToEndOf(recipient)` | Move the first element | ★☆☆ |
| `MoveLastToFrontOf(recipient)` | Move the last element | ★☆☆ |

### Task 3: B+ Tree Core Operations

**File:** `src/storage/index/b_plus_tree.cpp`

| Method | Description | Difficulty |
|--------|-------------|------------|
| `IsEmpty()` | Check whether the tree is empty | ★☆☆ |
| `GetValue(key, result)` | Exact key lookup | ★★☆ |
| `Insert(key, value)` | Insert key-value, handle splits | ★★★★ |
| `Remove(key)` | Remove key, handle merges/redistributions | ★★★★★ |

### Task 4: B+ Tree Iterator

**File:** `src/storage/index/b_plus_tree_iterator.cpp`

| Method | Description | Difficulty |
|--------|-------------|------------|
| `operator*()` | Return the current key-value pair | ★☆☆ |
| `operator++()` | Advance to the next key-value | ★★☆ |
| `operator==` / `operator!=` | Compare two iterators | ★☆☆ |
| `IsEnd()` | Check whether the end has been reached | ★☆☆ |

> **Note:** The current iterator design does not include a `BufferPoolManager*` member, which means the iterator cannot fetch pages through BPM. If you need to traverse across leaf nodes, you will need to modify the iterator header file to add the necessary member variables.

## 4. Implementation Notes

- Keep the node header fields, sizes, and parent links consistent whenever a page changes.
- Preserve sorted order inside each page and maintain the leaf-level linked list.
- Handle root split, root shrink, redistribution, and merge cases explicitly.
- Always fetch pages through the buffer pool and unpin them after use.
- The iterator should move across leaf pages without exposing internal page layout details.

## 5. Building and Testing

```bash
# Build the project
cd build && cmake --build . -j$(nproc)

# Run Lab 2 related tests
ctest --test-dir build -R b_plus_tree_internal_page_test --output-on-failure
ctest --test-dir build -R b_plus_tree_leaf_page_test --output-on-failure
ctest --test-dir build -R b_plus_tree_test --output-on-failure
ctest --test-dir build -R b_plus_tree_iterator_test --output-on-failure

# Run all tests
ctest --test-dir build --output-on-failure
```

### Debugging Tips

- Use print statements to output the tree structure: traverse each level and print keys and sizes
- Test simple sequential insertions first, then out-of-order/random insertions
- Implement `GetValue` + `Insert` (without split) first to verify basic functionality
- Then add split logic
- Finally implement deletion (the most complex part)

## 6. Common Mistakes

1. **Forgetting to UnpinPage**: After every `FetchPage` or `NewPage`, the page must be unpinned with `UnpinPage` when done. Missing this will cause buffer pool overflow
2. **Internal node key[0] is invalid**: The `array_[0].first` of an internal node does not store a valid key; only `array_[0].second` (child page pointer) is valid. Key traversal starts from index=1
3. **Forgetting to update parent pointers after split**: The children of newly created nodes have not had their `parent_page_id_` updated
4. **Leaf node linked list maintenance**: When splitting a leaf node, the new node's `next_page_id_` should point to the original node's old `next_page_id_`, and the original node's `next_page_id_` should be updated to the new node's page_id
5. **Choosing the key to push up during split**:
   - Leaf split: push up the first key of the new node (the key is still retained in the leaf)
   - Internal split: push up the middle key (the key is removed from the internal node)
6. **Merge direction**: When merging, make sure to update the parent node's key and child pointers, as well as the `parent_page_id_` of the merged node's children
7. **Root node special handling**: Deletion may cause the root node to have only one child remaining; in this case, the child should be promoted to become the new root
8. **Difference between max_size and min_size**: During insertion, check `size > max_size` (insert first, then check); during deletion, check `size < min_size`

## 7. Grading Criteria

| Component | Points |
|-----------|--------|
| Internal Page (12 methods) | 20 pts |
| Leaf Page (10 methods) | 20 pts |
| B+ Tree Search (GetValue) | 10 pts |
| B+ Tree Insert (Insert + Split) | 25 pts |
| B+ Tree Delete (Remove + Merge/Redistribute) | 15 pts |
| B+ Tree Iterator | 10 pts |
| **Total** | **100 pts** |
