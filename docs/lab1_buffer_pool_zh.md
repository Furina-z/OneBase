# Lab 1: 缓冲池管理器 (Buffer Pool Manager)

## 1. 实验概述

本实验的目标是为 OneBase 数据库实现一个完整的**缓冲池管理器 (Buffer Pool Manager)**。缓冲池是数据库管理系统 (DBMS) 中最核心的组件之一——它管理磁盘页面在内存中的缓存，使得上层组件可以透明地访问数据页，而无需直接与磁盘交互。

你将实现以下三个组件：

1. **LRU-K 替换器** (`src/buffer/lru_k_replacer.cpp`) — 决定当缓冲池满时淘汰哪一页
2. **缓冲池管理器** (`src/buffer/buffer_pool_manager.cpp`) — 管理内存中的页面帧，提供 NewPage / FetchPage / UnpinPage 等接口
3. **Page Guard** (`src/buffer/page_guard.cpp`) — RAII 风格的页面访问守卫，自动管理 pin/unpin 和读写锁

## 2. 背景知识

### 2.1 缓冲池架构

```
┌─────────────────────────────────────────────────────┐
│                  Buffer Pool Manager                 │
│                                                     │
│  ┌──────────┐   ┌──────────────────────────────┐   │
│  │ Page     │   │      Page Frames              │   │
│  │ Table    │   │  ┌───────┬───────┬───────┐   │   │
│  │ (HashMap)│──▶│  │Frame 0│Frame 1│Frame 2│...│   │
│  │pid→fid  │   │  └───────┴───────┴───────┘   │   │
│  └──────────┘   └──────────────────────────────┘   │
│                                                     │
│  ┌──────────┐   ┌──────────────┐                   │
│  │Free List │   │ LRU-K        │                   │
│  │(空闲帧)  │   │ Replacer     │                   │
│  └──────────┘   └──────────────┘                   │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │           DiskManager (磁盘I/O)              │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### 2.2 LRU-K 替换策略

LRU-K 是 LRU 的改进版本。对于每一帧，我们跟踪其最近 k 次访问的时间戳。

**Backward K-Distance 定义：**
- 如果一帧的访问次数 < k，则其 k-distance = +∞（正无穷）
- 如果访问次数 ≥ k，则 k-distance = 当前时间 - 第 k 次最早访问时间

**淘汰优先级：**
1. 优先淘汰 k-distance = +∞ 的帧（访问次数不足 k 的帧）
2. 在这些帧中，淘汰**最早首次访问**的帧
3. 如果所有帧都有 k 次访问记录，淘汰 k-distance **最大**的帧
4. 只考虑标记为 `evictable` 的帧

### 2.3 关键数据结构

**LRUKReplacer 的内部结构：**
```cpp
struct FrameEntry {
    std::list<size_t> history_;   // 最近 k 次访问的时间戳
    bool is_evictable_{false};    // 是否可被淘汰
};
std::unordered_map<frame_id_t, FrameEntry> entries_;
size_t current_timestamp_{0};     // 全局时间戳
size_t curr_size_{0};             // 当前可淘汰帧的数量
```

**BufferPoolManager 的内部结构：**
```cpp
Page *pages_;                                         // 页面帧数组 (大小 = pool_size_)
std::unordered_map<page_id_t, frame_id_t> page_table_; // 页面ID → 帧ID 映射
std::list<frame_id_t> free_list_;                     // 空闲帧列表
std::unique_ptr<LRUKReplacer> replacer_;              // 替换器
DiskManager *disk_manager_;                           // 磁盘管理器
```

### 2.4 Page Guard (RAII 守卫)

Page Guard 使用 C++ RAII 模式自动管理页面的生命周期：

| Guard 类型 | 构造时 | Drop() 时 |
|-----------|--------|-----------|
| BasicPageGuard | 无锁操作 | UnpinPage(pid, is_dirty_) |
| ReadPageGuard | page->RLatch() | page->RUnlatch() + UnpinPage(pid, false) |
| WritePageGuard | page->WLatch() | page->WUnlatch() + UnpinPage(pid, true) |

## 3. 你的任务

### Task 1: LRU-K Replacer

**文件：** `src/buffer/lru_k_replacer.cpp`

| 方法 | 说明 | 难度 |
|------|------|------|
| `Evict(frame_id_t *)` | 找到并淘汰 k-distance 最大的帧 | ★★★ |
| `RecordAccess(frame_id_t)` | 为帧记录一次新的访问时间戳 | ★☆☆ |
| `SetEvictable(frame_id_t, bool)` | 设置帧是否可被淘汰 | ★☆☆ |
| `Remove(frame_id_t)` | 从替换器中移除一帧 | ★☆☆ |
| `Size()` | 返回可淘汰帧的数量 | ★☆☆ |

### Task 2: Buffer Pool Manager

**文件：** `src/buffer/buffer_pool_manager.cpp`

| 方法 | 说明 | 难度 |
|------|------|------|
| `NewPage(page_id_t *)` | 分配一个新页面 | ★★★ |
| `FetchPage(page_id_t)` | 获取已有页面 | ★★★ |
| `UnpinPage(page_id_t, bool)` | 取消页面引用 | ★★☆ |
| `DeletePage(page_id_t)` | 删除页面 | ★★☆ |
| `FlushPage(page_id_t)` | 将页面写回磁盘 | ★☆☆ |
| `FlushAllPages()` | 写回所有页面 | ★☆☆ |

### Task 3: Page Guard

**文件：** `src/buffer/page_guard.cpp`

需要为 BasicPageGuard, ReadPageGuard, WritePageGuard 各实现：

| 方法 | 说明 | 难度 |
|------|------|------|
| `operator=(Guard &&)` | 移动赋值运算符 | ★★☆ |
| `Drop()` | 释放资源（解锁 + 取消引用） | ★★☆ |

## 4. 实现指南

### 4.1 LRU-K Replacer

**`RecordAccess(frame_id)`：**
1. 进入时获取 `latch_` 锁
2. 在 `entries_[frame_id]` 中追加 `current_timestamp_` 到 `history_`
3. 如果 `history_` 的大小超过 k，弹出最旧的时间戳：`history_.pop_front()`
4. 递增 `current_timestamp_++`

**`Evict(frame_id_t *frame_id)`：**
1. 获取锁
2. 遍历所有 `entries_`，只考虑 `is_evictable_ == true` 的帧
3. 分两类处理：
   - **+∞ 帧**（`history_.size() < k_`）：在这些帧中找到 `history_.front()`（首次访问时间）最小的
   - **有限 k-distance 帧**（`history_.size() >= k_`）：k-distance = `current_timestamp_ - history_.front()`，找最大的
4. 如果存在 +∞ 帧，优先淘汰它们（即使有限帧的 k-distance 更大也是如此）
5. 淘汰选中的帧：从 `entries_` 中删除，`curr_size_--`

**`SetEvictable(frame_id, set_evictable)`：**
1. 获取锁
2. 找到 `entries_[frame_id]`，如果不存在则直接返回
3. 如果从 evictable → non-evictable，`curr_size_--`
4. 如果从 non-evictable → evictable，`curr_size_++`
5. 更新 `entry.is_evictable_`

**`Remove(frame_id)`：**
1. 获取锁
2. 如果帧不存在，直接返回
3. 如果帧不是 evictable，抛出异常
4. `curr_size_--`，删除 entry

### 4.2 Buffer Pool Manager

**`NewPage(page_id_t *page_id)`：**
1. 获取 `latch_` 锁
2. 获取一个空闲帧：
   - 先尝试 `free_list_`（取 front）
   - 如果 free_list_ 为空，调用 `replacer_->Evict(&frame_id)`
   - 如果都失败，返回 `nullptr`
3. 如果是淘汰得到的帧，检查旧页面是否 dirty——如果是，先写回磁盘
4. 从 `page_table_` 中删除旧映射
5. 通过 `disk_manager_->AllocatePage()` 获取新 page_id
6. 初始化帧：`ResetMemory()`，设置 `page_id_`、`pin_count_ = 1`、`is_dirty_ = false`
7. 更新 `page_table_[page_id] = frame_id`
8. 调用 `replacer_->RecordAccess(frame_id)` 和 `replacer_->SetEvictable(frame_id, false)`

**`FetchPage(page_id_t page_id)`：**
1. 获取锁
2. 检查 `page_table_`——如果页面已在缓冲池中：
   - `pin_count_++`
   - `RecordAccess` + `SetEvictable(false)`
   - 返回页面指针
3. 如果不在缓冲池，获取空闲帧（同 NewPage 的逻辑）
4. 调用 `disk_manager_->ReadPage(page_id, page.data_)` 从磁盘读入
5. 更新 page 元数据和 page_table_

**`UnpinPage(page_id_t page_id, bool is_dirty)`：**
1. 在 page_table_ 中查找，不存在则返回 false
2. 如果 `pin_count_ <= 0`，返回 false
3. 如果 `is_dirty` 为 true，设置 `page.is_dirty_ = true`（注意：false 不应该覆盖已有的 dirty 状态）
4. `pin_count_--`
5. 当 `pin_count_` 降为 0 时，调用 `replacer_->SetEvictable(frame_id, true)`

### 4.3 Page Guard

**BasicPageGuard::Drop()：**
```
if (page_ == nullptr) return;
bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
bpm_ = nullptr;
page_ = nullptr;
is_dirty_ = false;
```

**ReadPageGuard::Drop()：**
```
if (page_ == nullptr) return;
page_->RUnlatch();      // 释放读锁
bpm_->UnpinPage(page_->GetPageId(), false);
bpm_ = nullptr;
page_ = nullptr;
```

**WritePageGuard::Drop()：**
```
if (page_ == nullptr) return;
page_->WUnlatch();      // 释放写锁
bpm_->UnpinPage(page_->GetPageId(), true);   // 总是 dirty
bpm_ = nullptr;
page_ = nullptr;
```

**移动赋值运算符（以 BasicPageGuard 为例）：**
```
if (this != &that) {
    Drop();              // 先释放自己当前持有的资源
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;  // 移动源置空
    that.page_ = nullptr;
}
return *this;
```

## 5. 编译与测试

```bash
# 编译项目
cd build && cmake --build . -j$(nproc)

# 运行 Lab 1 相关测试
ctest --test-dir build -R lru_k_replacer_test --output-on-failure
ctest --test-dir build -R buffer_pool_manager_test --output-on-failure
ctest --test-dir build -R page_guard_test --output-on-failure

# 运行全部测试
ctest --test-dir build --output-on-failure
```

## 6. 常见错误

1. **忘记写回脏页**：在淘汰帧之前，如果旧页面是 dirty 的，必须先调用 `disk_manager_->WritePage()` 写回磁盘
2. **`curr_size_` 不同步**：在 `SetEvictable` 中必须正确地增减 `curr_size_`，否则 `Size()` 返回值会不正确
3. **PageGuard 双重释放**：移动构造函数必须将源对象的指针置为 nullptr，否则移动后两个对象的析构都会调用 Drop()
4. **FetchPage 忘记 RecordAccess**：无论页面是否在缓冲池中，FetchPage 都必须调用 `RecordAccess` 和 `SetEvictable(false)`
5. **UnpinPage 覆盖 dirty 状态**：`is_dirty = false` 不应该清除已有的 dirty 标记。只有 `is_dirty = true` 时才更新
6. **线程安全**：所有公共方法都需要获取 `latch_` 锁。注意 `Size()` 不需要锁（因为 `curr_size_` 是原子更新的）

## 7. 评分标准

| 组件 | 分值 |
|------|------|
| LRU-K Replacer (5 个方法) | 30 分 |
| Buffer Pool Manager (6 个方法) | 40 分 |
| Page Guard (3 类 × 2 个方法) | 30 分 |
| **总计** | **100 分** |
