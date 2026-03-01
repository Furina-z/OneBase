# Lab 1: Buffer Pool Manager

## 1. Overview

The goal of this lab is to implement a complete **Buffer Pool Manager** for the OneBase database. The buffer pool is one of the most critical components in a database management system (DBMS) -- it manages the caching of disk pages in memory, allowing upper-layer components to transparently access data pages without directly interacting with the disk.

You will implement the following three components:

1. **LRU-K Replacer** (`src/buffer/lru_k_replacer.cpp`) -- Decides which page to evict when the buffer pool is full
2. **Buffer Pool Manager** (`src/buffer/buffer_pool_manager.cpp`) -- Manages page frames in memory, providing interfaces such as NewPage / FetchPage / UnpinPage
3. **Page Guard** (`src/buffer/page_guard.cpp`) -- RAII-style page access guard that automatically manages pin/unpin and read-write locks

## 2. Background

### 2.1 Buffer Pool Architecture

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
│  │(Free     │   │ Replacer     │                   │
│  │ Frames)  │   └──────────────┘                   │
│  └──────────┘                                       │
│                                                     │
│  ┌─────────────────────────────────────────────┐   │
│  │           DiskManager (Disk I/O)             │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

### 2.2 LRU-K Replacement Policy

LRU-K is an improved version of LRU. For each frame, we track the timestamps of its most recent k accesses.

**Backward K-Distance Definition:**
- If a frame has been accessed fewer than k times, its k-distance = +infinity
- If the access count is >= k, then k-distance = current time - the timestamp of the k-th earliest access

**Eviction Priority:**
1. Frames with k-distance = +infinity (frames accessed fewer than k times) are evicted first
2. Among these frames, evict the one with the **earliest first access**
3. If all frames have k access records, evict the one with the **largest** k-distance
4. Only frames marked as `evictable` are considered

### 2.3 Key Data Structures

**Internal structure of LRUKReplacer:**
```cpp
struct FrameEntry {
    std::list<size_t> history_;   // Timestamps of the most recent k accesses
    bool is_evictable_{false};    // Whether this frame can be evicted
};
std::unordered_map<frame_id_t, FrameEntry> entries_;
size_t current_timestamp_{0};     // Global timestamp
size_t curr_size_{0};             // Current number of evictable frames
```

**Internal structure of BufferPoolManager:**
```cpp
Page *pages_;                                         // Page frame array (size = pool_size_)
std::unordered_map<page_id_t, frame_id_t> page_table_; // Page ID → Frame ID mapping
std::list<frame_id_t> free_list_;                     // Free frame list
std::unique_ptr<LRUKReplacer> replacer_;              // Replacer
DiskManager *disk_manager_;                           // Disk manager
```

### 2.4 Page Guard (RAII Guard)

Page Guard uses the C++ RAII pattern to automatically manage the lifecycle of pages:

| Guard Type | On Construction | On Drop() |
|-----------|-----------------|-----------|
| BasicPageGuard | No lock operation | UnpinPage(pid, is_dirty_) |
| ReadPageGuard | page->RLatch() | page->RUnlatch() + UnpinPage(pid, false) |
| WritePageGuard | page->WLatch() | page->WUnlatch() + UnpinPage(pid, true) |

## 3. Your Tasks

### Task 1: LRU-K Replacer

**File:** `src/buffer/lru_k_replacer.cpp`

| Method | Description | Difficulty |
|--------|-------------|------------|
| `Evict(frame_id_t *)` | Find and evict the frame with the largest k-distance | ★★★ |
| `RecordAccess(frame_id_t)` | Record a new access timestamp for a frame | ★☆☆ |
| `SetEvictable(frame_id_t, bool)` | Set whether a frame is evictable | ★☆☆ |
| `Remove(frame_id_t)` | Remove a frame from the replacer | ★☆☆ |
| `Size()` | Return the number of evictable frames | ★☆☆ |

### Task 2: Buffer Pool Manager

**File:** `src/buffer/buffer_pool_manager.cpp`

| Method | Description | Difficulty |
|--------|-------------|------------|
| `NewPage(page_id_t *)` | Allocate a new page | ★★★ |
| `FetchPage(page_id_t)` | Fetch an existing page | ★★★ |
| `UnpinPage(page_id_t, bool)` | Unpin a page | ★★☆ |
| `DeletePage(page_id_t)` | Delete a page | ★★☆ |
| `FlushPage(page_id_t)` | Flush a page to disk | ★☆☆ |
| `FlushAllPages()` | Flush all pages to disk | ★☆☆ |

### Task 3: Page Guard

**File:** `src/buffer/page_guard.cpp`

You need to implement the following for each of BasicPageGuard, ReadPageGuard, and WritePageGuard:

| Method | Description | Difficulty |
|--------|-------------|------------|
| `operator=(Guard &&)` | Move assignment operator | ★★☆ |
| `Drop()` | Release resources (unlock + unpin) | ★★☆ |

## 4. Implementation Guide

### 4.1 LRU-K Replacer

**`RecordAccess(frame_id)`:**
1. Acquire the `latch_` lock upon entry
2. Append `current_timestamp_` to `history_` in `entries_[frame_id]`
3. If the size of `history_` exceeds k, pop the oldest timestamp: `history_.pop_front()`
4. Increment `current_timestamp_++`

**`Evict(frame_id_t *frame_id)`:**
1. Acquire the lock
2. Iterate over all `entries_`, only considering frames where `is_evictable_ == true`
3. Handle two categories:
   - **+infinity frames** (`history_.size() < k_`): Among these frames, find the one with the smallest `history_.front()` (earliest first access time)
   - **Finite k-distance frames** (`history_.size() >= k_`): k-distance = `current_timestamp_ - history_.front()`, find the largest
4. If +infinity frames exist, evict them first (even if finite frames have a larger k-distance)
5. Evict the selected frame: remove from `entries_`, `curr_size_--`

**`SetEvictable(frame_id, set_evictable)`:**
1. Acquire the lock
2. Find `entries_[frame_id]`; if it does not exist, return immediately
3. If changing from evictable to non-evictable, `curr_size_--`
4. If changing from non-evictable to evictable, `curr_size_++`
5. Update `entry.is_evictable_`

**`Remove(frame_id)`:**
1. Acquire the lock
2. If the frame does not exist, return immediately
3. If the frame is not evictable, throw an exception
4. `curr_size_--`, delete the entry

### 4.2 Buffer Pool Manager

**`NewPage(page_id_t *page_id)`:**
1. Acquire the `latch_` lock
2. Obtain a free frame:
   - First try `free_list_` (take from front)
   - If free_list_ is empty, call `replacer_->Evict(&frame_id)`
   - If both fail, return `nullptr`
3. If the frame was obtained via eviction, check whether the old page is dirty -- if so, flush it to disk first
4. Remove the old mapping from `page_table_`
5. Obtain a new page_id via `disk_manager_->AllocatePage()`
6. Initialize the frame: `ResetMemory()`, set `page_id_`, `pin_count_ = 1`, `is_dirty_ = false`
7. Update `page_table_[page_id] = frame_id`
8. Call `replacer_->RecordAccess(frame_id)` and `replacer_->SetEvictable(frame_id, false)`

**`FetchPage(page_id_t page_id)`:**
1. Acquire the lock
2. Check `page_table_` -- if the page is already in the buffer pool:
   - `pin_count_++`
   - `RecordAccess` + `SetEvictable(false)`
   - Return the page pointer
3. If not in the buffer pool, obtain a free frame (same logic as NewPage)
4. Call `disk_manager_->ReadPage(page_id, page.data_)` to read from disk
5. Update page metadata and page_table_

**`UnpinPage(page_id_t page_id, bool is_dirty)`:**
1. Look up in page_table_; if not found, return false
2. If `pin_count_ <= 0`, return false
3. If `is_dirty` is true, set `page.is_dirty_ = true` (note: false should not overwrite an existing dirty state)
4. `pin_count_--`
5. When `pin_count_` drops to 0, call `replacer_->SetEvictable(frame_id, true)`

### 4.3 Page Guard

**BasicPageGuard::Drop():**
```
if (page_ == nullptr) return;
bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
bpm_ = nullptr;
page_ = nullptr;
is_dirty_ = false;
```

**ReadPageGuard::Drop():**
```
if (page_ == nullptr) return;
page_->RUnlatch();      // Release read lock
bpm_->UnpinPage(page_->GetPageId(), false);
bpm_ = nullptr;
page_ = nullptr;
```

**WritePageGuard::Drop():**
```
if (page_ == nullptr) return;
page_->WUnlatch();      // Release write lock
bpm_->UnpinPage(page_->GetPageId(), true);   // Always dirty
bpm_ = nullptr;
page_ = nullptr;
```

**Move Assignment Operator (using BasicPageGuard as an example):**
```
if (this != &that) {
    Drop();              // First release resources currently held by this object
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;  // Nullify the move source
    that.page_ = nullptr;
}
return *this;
```

## 5. Building and Testing

```bash
# Build the project
cd build && cmake --build . -j$(nproc)

# Run Lab 1 related tests
ctest --test-dir build -R lru_k_replacer_test --output-on-failure
ctest --test-dir build -R buffer_pool_manager_test --output-on-failure
ctest --test-dir build -R page_guard_test --output-on-failure

# Run all tests
ctest --test-dir build --output-on-failure
```

## 6. Common Mistakes

1. **Forgetting to flush dirty pages**: Before evicting a frame, if the old page is dirty, you must first call `disk_manager_->WritePage()` to flush it to disk
2. **`curr_size_` out of sync**: You must correctly increment/decrement `curr_size_` in `SetEvictable`, otherwise `Size()` will return incorrect values
3. **PageGuard double free**: The move constructor must set the source object's pointers to nullptr; otherwise both objects' destructors will call Drop() after the move
4. **FetchPage missing RecordAccess**: Regardless of whether the page is already in the buffer pool, FetchPage must call `RecordAccess` and `SetEvictable(false)`
5. **UnpinPage overwriting dirty state**: `is_dirty = false` should not clear an existing dirty flag. Only update when `is_dirty = true`
6. **Thread safety**: All public methods must acquire the `latch_` lock. Note that `Size()` does not need the lock (because `curr_size_` is updated atomically)

## 7. Grading Criteria

| Component | Points |
|-----------|--------|
| LRU-K Replacer (5 methods) | 30 points |
| Buffer Pool Manager (6 methods) | 40 points |
| Page Guard (3 classes x 2 methods) | 30 points |
| **Total** | **100 points** |
