# Lab 2: B+ 树索引 (B+ Tree Index)

## 1. 实验概述

本实验的目标是为 OneBase 数据库实现一个完整的 **B+ 树索引 (B+ Tree Index)**。B+ 树是关系数据库中最广泛使用的索引结构——它支持高效的等值查询和范围查询，查找、插入、删除的时间复杂度均为 O(log n)。

你将实现以下四个组件：

1. **Internal Page** (`src/storage/page/b_plus_tree_internal_page.cpp`) — B+ 树内部节点页面
2. **Leaf Page** (`src/storage/page/b_plus_tree_leaf_page.cpp`) — B+ 树叶子节点页面
3. **B+ Tree** (`src/storage/index/b_plus_tree.cpp`) — B+ 树的核心操作（查找、插入、删除）
4. **B+ Tree Iterator** (`src/storage/index/b_plus_tree_iterator.cpp`) — 支持顺序遍历叶子节点

## 2. 背景知识

### 2.1 B+ 树结构

```
                    ┌─────────────┐
                    │  Internal   │
                    │  [_, 5, 10] │        key[0] 无效
                    │ p0  p1  p2  │        value[i] 是 child page_id
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

**核心特性：**
- 所有数据存储在叶子节点中
- 内部节点仅存储路由键 (routing keys) + 子节点指针
- 叶子节点之间通过 `next_page_id_` 形成单向链表
- 内部节点的 `array_[0].first`（第一个 key）是无效的，仅 `array_[0].second` (指针) 有效

### 2.2 页面布局

每个 B+ 树节点存储在一个数据库页面 (Page) 中。页面的内存布局如下：

```
┌─────────────────── Page (4096 bytes) ────────────────────┐
│                                                          │
│  BPlusTreePage header:                                   │
│    page_type_     (IndexPageType: LEAF or INTERNAL)      │
│    size_          (当前 key-value 对数量)                  │
│    max_size_      (最大容量)                               │
│    parent_page_id_ (父节点 page_id)                       │
│                                                          │
│  [Leaf only] next_page_id_  (下一个叶节点)                 │
│                                                          │
│  array_[0]:  { key_0, value_0 }                          │
│  array_[1]:  { key_1, value_1 }                          │
│  ...                                                     │
│  array_[n-1]: { key_{n-1}, value_{n-1} }                 │
└──────────────────────────────────────────────────────────┘
```

**重要细节：**
- `array_[0]` 使用 C 语言的 flexible array member 技巧（声明为 `std::pair<KeyType, ValueType> array_[0]`）
- 实际可用的数组大小由 `max_size_` 决定，通过 `reinterpret_cast` 将 Page 的 `data_` 区域视为节点
- 对于内部节点：`ValueType` = `page_id_t`（子页面 ID）
- 对于叶子节点：`ValueType` = `RID`（记录标识符）

### 2.3 分裂与合并

**分裂 (Split)：** 当节点插入后 `size > max_size` 时触发

```
Before split (leaf, max_size=4):
  [1, 2, 3, 4, 5]     ← size=5 > max_size=4, 需要分裂

After split:
  原节点: [1, 2]       ← 保留前半部分
  新节点: [3, 4, 5]    ← 后半部分移到新节点
  向上插入: key=3 到父节点
```

**合并 (Merge)：** 当节点删除后 `size < min_size` 时触发
- `min_size` = `max_size / 2`（叶子节点）或 `(max_size + 1) / 2`（内部节点）
- 先尝试从兄弟节点**重新分配 (Redistribute)**
- 如果兄弟节点也不富裕，则**合并 (Merge)** 两个节点

### 2.4 KeyComparator

B+ 树是模板类 `BPlusTree<KeyType, ValueType, KeyComparator>`，在本项目中的默认实例化为：
- `KeyType` = `int`
- `ValueType` = `RID`
- `KeyComparator` = `std::less<int>`

`comparator(a, b)` 返回 `true` 表示 `a < b`。

## 3. 你的任务

### Task 1: Internal Page 方法

**文件：** `src/storage/page/b_plus_tree_internal_page.cpp`

| 方法 | 说明 | 难度 |
|------|------|------|
| `Init(max_size)` | 初始化页面元数据 | ★☆☆ |
| `KeyAt(index)` / `SetKeyAt(index, key)` | 读/写指定位置的 key | ★☆☆ |
| `ValueAt(index)` / `SetValueAt(index, value)` | 读/写指定位置的 value | ★☆☆ |
| `ValueIndex(value)` | 找到给定 value (child page_id) 的下标 | ★☆☆ |
| `Lookup(key, comparator)` | 找到 key 应该去的子节点 page_id | ★★☆ |
| `PopulateNewRoot(old_val, key, new_val)` | 初始化新的根节点 | ★☆☆ |
| `InsertNodeAfter(old_val, key, new_val)` | 在 old_val 后面插入新的 key-value | ★★☆ |
| `Remove(index)` | 删除指定位置的 key-value | ★☆☆ |
| `RemoveAndReturnOnlyChild()` | 删除根节点并返回唯一子节点 | ★☆☆ |
| `MoveAllTo(recipient, middle_key)` | 将所有元素移动到 recipient | ★★☆ |
| `MoveHalfTo(recipient, middle_key)` | 将后半部分移动到 recipient（分裂用） | ★★☆ |
| `MoveFirstToEndOf(recipient, middle_key)` | 将第一个元素移到 recipient 末尾 | ★★☆ |
| `MoveLastToFrontOf(recipient, middle_key)` | 将最后一个元素移到 recipient 开头 | ★★☆ |

### Task 2: Leaf Page 方法

**文件：** `src/storage/page/b_plus_tree_leaf_page.cpp`

| 方法 | 说明 | 难度 |
|------|------|------|
| `Init(max_size)` | 初始化页面元数据 | ★☆☆ |
| `KeyAt(index)` / `ValueAt(index)` | 读取指定位置的 key/value | ★☆☆ |
| `KeyIndex(key, comparator)` | 二分查找第一个 ≥ key 的位置 | ★★☆ |
| `Lookup(key, value, comparator)` | 精确查找 key 对应的 value | ★★☆ |
| `Insert(key, value, comparator)` | 在有序位置插入 key-value | ★★★ |
| `RemoveAndDeleteRecord(key, comparator)` | 删除指定 key | ★★☆ |
| `MoveHalfTo(recipient)` | 将后半部分移到 recipient（分裂用） | ★★☆ |
| `MoveAllTo(recipient)` | 将所有元素移到 recipient（合并用） | ★★☆ |
| `MoveFirstToEndOf(recipient)` | 移动第一个元素 | ★☆☆ |
| `MoveLastToFrontOf(recipient)` | 移动最后一个元素 | ★☆☆ |

### Task 3: B+ Tree 核心操作

**文件：** `src/storage/index/b_plus_tree.cpp`

| 方法 | 说明 | 难度 |
|------|------|------|
| `IsEmpty()` | 判断树是否为空 | ★☆☆ |
| `GetValue(key, result)` | 精确查找 key | ★★☆ |
| `Insert(key, value)` | 插入 key-value，处理分裂 | ★★★★ |
| `Remove(key)` | 删除 key，处理合并/重分配 | ★★★★★ |

### Task 4: B+ Tree Iterator

**文件：** `src/storage/index/b_plus_tree_iterator.cpp`

| 方法 | 说明 | 难度 |
|------|------|------|
| `operator*()` | 返回当前 key-value 对 | ★☆☆ |
| `operator++()` | 前进到下一个 key-value | ★★☆ |
| `operator==` / `operator!=` | 比较两个迭代器 | ★☆☆ |
| `IsEnd()` | 判断是否到达末尾 | ★☆☆ |

> **注意：** 当前迭代器设计中没有 `BufferPoolManager*` 成员，这意味着迭代器无法通过 BPM 获取页面。如果你需要跨叶节点遍历，你需要修改迭代器头文件添加必要的成员变量。

## 4. 实现指南

### 4.1 Internal Page

**`Init(max_size)`：**
```
SetPageType(IndexPageType::INTERNAL_PAGE);
SetSize(0);
SetMaxSize(max_size);
SetParentPageId(INVALID_PAGE_ID);
```

**`Lookup(key, comparator)`：**

在内部节点中查找 key 应该路由到哪个子节点。

数组布局：`[_, k1, k2, k3, ...]`，对应值 `[p0, p1, p2, p3, ...]`
- 如果 `key < k1`，去 p0
- 如果 `k1 ≤ key < k2`，去 p1
- 依此类推

```
result = array_[0].second   // 默认去第一个子节点
for i = 1 to size-1:
    if comparator(key, array_[i].first):   // key < array_[i].key
        break
    result = array_[i].second
return result
```

**`InsertNodeAfter(old_value, key, new_value)`：**
1. 找到 `old_value` 的下标 `idx`
2. 将 `[idx+1, size)` 的元素都向后移一位
3. 在 `idx+1` 位置放入 `{key, new_value}`
4. `IncreaseSize(1)`

**`MoveHalfTo(recipient, middle_key)`：**
```
split_idx = size / 2   // 对于内部节点
把 array_[split_idx+1 .. size-1] 复制到 recipient
recipient 的第一个 key 设为 middle_key（即原本 array_[split_idx].key 会被推上父节点）
调整 size
```

### 4.2 Leaf Page

**`KeyIndex(key, comparator)`：**

找到第一个 ≥ key 的位置（排序插入点）。可以使用线性扫描或二分查找：

```
for i = 0 to size-1:
    if !comparator(array_[i].first, key):  // array_[i].key >= key
        return i
return size
```

**`Insert(key, value, comparator)`：**
1. 调用 `KeyIndex(key)` 找到插入位置 `idx`
2. 如果 `idx < size && array_[idx].key == key`，返回当前 size（重复键，不插入）
3. 将 `[idx, size)` 的元素向后移一位
4. 在 `idx` 位置放入 `{key, value}`
5. `IncreaseSize(1)`

**`MoveHalfTo(recipient)`：**
```
split_idx = size / 2
将 array_[split_idx .. size-1] 复制到 recipient 的 array_[0..]
更新 recipient 的 next_page_id_ = 自己原来的 next_page_id_
将自己的 next_page_id_ 设为 recipient 所在的 page_id
调整两个节点的 size
```

### 4.3 B+ Tree 核心操作

**`GetValue(key, result)`：**
1. 如果树为空，返回 false
2. 从根节点开始，使用 `Lookup()` 向下查找直到叶子节点
3. 在叶子节点中调用 `Lookup(key, &value)` 查找
4. 找到则将 value 放入 result vector，返回 true

**注意：** 每次访问一个页面，需要通过 `bpm_->FetchPage(page_id)` 获取页面，使用完后 `bpm_->UnpinPage(page_id, false)` 释放。

**`Insert(key, value)`：**

这是最复杂的方法。核心流程：

```
1. 如果树为空：
   a. 创建新叶子页面作为根
   b. 插入 key-value
   c. 返回 true

2. 查找叶子节点（从根向下遍历）

3. 在叶子节点中插入 key-value
   - 如果是重复键，返回 false

4. 如果叶子节点溢出（size > max_size）：
   a. 创建新叶子节点
   b. 调用 MoveHalfTo 分裂
   c. 将新叶子节点的第一个 key 上推到父节点
   d. InsertIntoParent(old_leaf, key, new_leaf)

5. InsertIntoParent 递归处理：
   a. 如果旧节点是根，创建新根
   b. 在父节点中调用 InsertNodeAfter
   c. 如果父节点也溢出，继续分裂父节点（递归）
```

**`Remove(key)`：**

```
1. 查找包含 key 的叶子节点
2. 在叶子中删除 key
3. 如果叶子 size < min_size，需要调整：
   a. 尝试从左兄弟或右兄弟重新分配 (Redistribute)
   b. 如果兄弟不富裕，与兄弟合并 (Merge)
   c. 合并可能导致父节点也需要调整（递归向上）
4. 特殊情况：如果根节点只剩一个子节点，用子节点替换根
```

### 4.4 B+ Tree Iterator

迭代器需要能够遍历所有叶子节点。基本设计：

```
成员变量：
  page_id_   当前叶节点的 page_id
  index_     当前在叶节点 array_ 中的下标

operator++():
  index_++
  如果 index_ >= 当前叶节点的 size：
    page_id_ = 当前叶节点的 next_page_id_
    index_ = 0

IsEnd():
  return page_id_ == INVALID_PAGE_ID
```

> **设计限制：** 当前迭代器头文件缺少 `BufferPoolManager*` 指针，所以 `operator*()` 和 `operator++()` 无法直接通过 BPM 获取页面数据。你需要在头文件中添加 `BufferPoolManager *bpm_` 成员来解决这个问题，或者考虑将必要数据缓存在迭代器中。

## 5. 编译与测试

```bash
# 编译项目
cd build && cmake --build . -j$(nproc)

# 运行 Lab 2 相关测试
ctest --test-dir build -R b_plus_tree_internal_page_test --output-on-failure
ctest --test-dir build -R b_plus_tree_leaf_page_test --output-on-failure
ctest --test-dir build -R b_plus_tree_test --output-on-failure
ctest --test-dir build -R b_plus_tree_iterator_test --output-on-failure

# 运行全部测试
ctest --test-dir build --output-on-failure
```

### 调试建议

- 使用打印语句输出树的结构：遍历每一层节点，打印 key 和 size
- 先测试简单的顺序插入，再测试乱序/随机插入
- 先实现 `GetValue` + `Insert`（不含分裂），验证基本功能
- 然后加入分裂逻辑
- 最后实现删除（最复杂）

## 6. 常见错误

1. **忘记 UnpinPage**：每次 `FetchPage` 或 `NewPage` 之后，用完页面必须 `UnpinPage`。遗漏会导致缓冲池溢出
2. **内部节点 key[0] 无效**：内部节点的 `array_[0].first` 不存储有效的 key，只有 `array_[0].second`（子页面指针）有效。遍历 key 从 index=1 开始
3. **分裂后忘记更新父指针**：新创建的节点的子节点没有更新 `parent_page_id_`
4. **叶子节点链表维护**：分裂叶子节点时，新节点的 `next_page_id_` 应该指向原节点的旧 `next_page_id_`，原节点的 `next_page_id_` 更新为新节点的 page_id
5. **分裂上推 key 的选择**：
   - 叶子分裂：上推新节点的第一个 key（该 key 在叶子中仍然保留）
   - 内部分裂：上推中间 key（该 key 从内部节点中移除）
6. **合并方向**：合并时要注意更新父节点中的 key 和子指针，以及被合并节点的子节点的 parent_page_id_
7. **根节点特殊处理**：删除可能导致根节点只剩一个子节点，此时应该将子节点提升为新的根
8. **max_size 和 min_size 的区别**：插入时检查 `size > max_size`（先插再查），删除时检查 `size < min_size`

## 7. 评分标准

| 组件 | 分值 |
|------|------|
| Internal Page (12 个方法) | 20 分 |
| Leaf Page (10 个方法) | 20 分 |
| B+ Tree 查找 (GetValue) | 10 分 |
| B+ Tree 插入 (Insert + 分裂) | 25 分 |
| B+ Tree 删除 (Remove + 合并/重分配) | 15 分 |
| B+ Tree Iterator | 10 分 |
| **总计** | **100 分** |
