# OneBase — AI Coding Agent Guide

OneBase is a **relational database management system** built from scratch in modern C++17. It's designed as an educational project with 4 progressive labs: buffer pool management, B+ tree indexing, query execution, and concurrency control.

## Quick Start

### Build & Test
```bash
mkdir build && cd build
cmake ..                    # AddressSanitizer enabled in Debug mode
cmake --build . -j$(nproc)

# Run all tests
ctest --test-dir build --output-on-failure

# Run specific lab evaluation (graded) tests
ctest --test-dir build -R lab1_eval_test --output-on-failure
ctest --test-dir build -R lab2_eval_test --output-on-failure
ctest --test-dir build -R lab3_eval_test --output-on-failure
ctest --test-dir build -R lab4_eval_test --output-on-failure
```

### Project Structure
```
src/
├── buffer/          Lab 1: Page caching, LRU-K replacer, RAII guards
├── storage/         Lab 2: B+ tree indexing, row storage (TableHeap), disk I/O
├── execution/       Lab 3: 11 query executors using Volcano iterator model
├── concurrency/     Lab 4: Two-phase locking, lock manager, transactions
├── catalog/         Table & index metadata
├── binder/          SQL semantic analysis
├── optimizer/       Query optimization
├── type/            SQL type system (INTEGER, VARCHAR, etc.)
├── common/          Utilities, types, config
└── include/onebase/ Public API headers
```

## Labs Overview

| Lab | Topic | Key Components | Documentation |
|-----|-------|---|---|
| 1 | Buffer Pool | LRU-K Replacer, BufferPoolManager, PageGuard (RAII) | [lab1_buffer_pool_en.md](docs/lab1_buffer_pool_en.md) |
| 2 | B+ Tree Index | BPlusTree template, InternalPage, LeafPage, Iterator | [lab2_b_plus_tree_en.md](docs/lab2_b_plus_tree_en.md) |
| 3 | Query Execution | 11 Executors, Volcano iterator model, join algorithms | [lab3_query_execution_en.md](docs/lab3_query_execution_en.md) |
| 4 | Concurrency | LockManager (row-level 2PL), TransactionManager | [lab4_concurrency_control_en.md](docs/lab4_concurrency_control_en.md) |

## Code Conventions & Patterns

### Naming
- **Classes**: PascalCase with role suffixes: `BufferPoolManager`, `SeqScanExecutor`, `BPlusTreeIterator`
- **Methods**: camelCase, prefer auto return types: `auto GetPageId() const -> page_id_t`
- **Members**: snake_case with trailing underscore (private): `page_id_`, `pin_count_`, `is_dirty_`
- **Type aliases** (from [common/types.h](src/include/onebase/common/types.h)): `page_id_t`, `frame_id_t`, `txn_id_t`, `table_oid_t`, `slot_offset_t`
- **Sentinel values**: `INVALID_PAGE_ID = -1`, `INVALID_FRAME_ID = -1`

### Essential Patterns

**1. RAII Page Management — Use PageGuard, Never Manual Pin/Unpin**
```cpp
// ✅ CORRECT: PageGuard auto-manages pin/unpin and locking
{
    auto guard = bpm->FetchPageWrite(page_id);  // pins + locks
    guard.AsMut<MyPage>()->ModifyData();
}  // Auto unpin + unlock on scope exit

// ❌ WRONG: Manual pin/unpin can leak or crash
auto page = bpm->FetchPage(page_id);
page->data = ...;
bpm->UnpinPage(page_id, true);  // Error-prone
```

**2. Volcano Iterator Model — All Executors**
```cpp
class AbstractExecutor {
    virtual void Init() = 0;  // Reset state, called once per execution
    virtual auto Next(Tuple *tuple, RID *rid) -> bool = 0;  // One row per call
    virtual auto GetOutputSchema() const -> const Schema& = 0;
};
```
- **Streaming** operators (SeqScan, Projection): produce one tuple per `Next()` call
- **Materializing** operators (Sort, Aggregation, HashJoin): buffer all child results before first `Next()`

**3. B+ Tree Templates — Generic with Comparator**
```cpp
template <typename KeyType, typename ValueType, typename KeyComparator>
class BPlusTree {
    auto Insert(const KeyType &key, const ValueType &value) -> bool;
};
```
- Always provide `KeyComparator` (e.g., `std::less<int>`) or ensure `operator<` exists
- Handle both integer and string keys (VARCHAR)

**4. Two-Phase Locking — Transaction Protocol**
- Phases: `GROWING` (acquire locks) → `SHRINKING` (release locks)
- Once `Unlock()` called, no new `Lock*()` allowed
- Lock modes: `SHARED` (compatible) vs. `EXCLUSIVE` (mutually exclusive)
- Lock upgrade: `SHARED` → `EXCLUSIVE` only if no other holders

**5. Table/B+ Tree Iteration**
```cpp
for (auto it = table->Begin(); it != table->End(); ++it) {
    Tuple t = *it;
    RID rid = it.GetRID();  // Page ID + slot offset
}
```
- Never call Insert/Remove/Update during iteration (invalidates iterator)

### Key Files to Reference
| File | Purpose |
|------|---------|
| [buffer_pool_manager.h](src/include/onebase/buffer/buffer_pool_manager.h) | Core buffer abstraction (NewPage, FetchPage, UnpinPage, DeletePage) |
| [page_guard.h](src/include/onebase/buffer/page_guard.h) | RAII guards (BasicPageGuard, ReadPageGuard, WritePageGuard) |
| [abstract_executor.h](src/include/onebase/execution/executors/abstract_executor.h) | Executor base class, Volcano model |
| [b_plus_tree.h](src/include/onebase/storage/index/b_plus_tree.h) | Generic B+ tree template with comparator |
| [table_heap.h](src/include/onebase/storage/table/table_heap.h) | Row storage with Iterator pattern |
| [lock_manager.h](src/include/onebase/concurrency/lock_manager.h) | Row-level 2PL locking |
| [rid.h](src/include/onebase/common/rid.h) | Record identifier (page_id + slot offset) |
| [types.h](src/include/onebase/common/types.h) | Type aliases and constants |
| [config.h](src/include/onebase/common/config.h) | Global constants (PAGE_SIZE=4096, etc.) |

## Common Pitfalls & How to Avoid Them

| Issue | Symptom | Fix |
|-------|---------|-----|
| **Not using PageGuard** | Memory corruption, pin count leaks | Always use `FetchPageRead()` / `FetchPageWrite()` with guard in scope |
| **Manual pin/unpin mismatch** | Crashes, resource exhaustion | Never call `UnpinPage()` directly; use PageGuard (RAII) |
| **B+ tree off-by-one errors** | Assertion failures during insert/delete | Remember: `array_[0]` key is invalid for internal nodes; test edge cases |
| **Iterator invalidation** | Crash during tree traversal | Never modify tree while iterating; break early if needed |
| **Lock deadlock** | Threads hang indefinitely | Always acquire locks in consistent order (e.g., by RID) |
| **Executor not reinitialized** | No output or duplicate rows | Call `Init()` once per execution, ensure it resets all state |
| **Tree modification memory leak** | Resource exhaustion over time | Pair all `NewPage()` with `DeletePage()`; verify in tests |
| **Conda environment interference** | CMake fails mysteriously | Run `conda deactivate` before building |

### Debugging Tips
- **Crashes & Assertions**: Check AddressSanitizer output (enabled in Debug mode)
- **Pin Count Issues**: Use `bpm->GetPinCount(page_id)` to verify it's 0 after scope exits
- **B+ Tree Corruption**: Print key ordering and parent pointers; verify leaf depth is uniform
- **Lock Deadlock**: Trace lock acquisition order; check for cyclic dependencies
- **Executor Problems**: Add debug prints in `Next()`; verify `Init()` called exactly once

## Lab-Specific Guidance

### Lab 1: Buffer Pool Manager
- Implement LRU-K replacement policy with configurable k-distance (default k=10)
- Use `PageGuard` for RAII page access
- Test pin/unpin balance by checking `GetPinCount()` == 0 after operations
- See: [lab1_buffer_pool_en.md](docs/lab1_buffer_pool_en.md), [buffer_pool_manager.h](src/include/onebase/buffer/buffer_pool_manager.h)

### Lab 2: B+ Tree Index
- Generic template with `KeyType`, `ValueType`, `KeyComparator` parameters
- Handle both integer and string keys; leaf nodes store key-value pairs; internal nodes store key pointers
- Implement Insert, Delete, Search, and Iterator with proper page allocation/deallocation
- Test split/merge edge cases (empty tree, single key, full pages)
- See: [lab2_b_plus_tree_en.md](docs/lab2_b_plus_tree_en.md), [b_plus_tree.h](src/include/onebase/storage/index/b_plus_tree.h)

### Lab 3: Query Execution
- Implement 11 executors: SeqScan, IndexScan, Insert, Delete, Update, NestedLoopJoin, HashJoin, Aggregation, Sort, Limit, Projection
- Follow Volcano iterator model: `Init()` once, `Next()` repeatedly until false
- Distinguish streaming (SeqScan, Projection) vs. materializing (Sort, Aggregation) operators
- Register executors in `ExecutorFactory::CreateExecutor()`
- See: [lab3_query_execution_en.md](docs/lab3_query_execution_en.md), [abstract_executor.h](src/include/onebase/execution/executors/abstract_executor.h)

### Lab 4: Concurrency Control
- Implement row-level two-phase locking (2PL) with lock compatibility matrix
- Support `LockShared()`, `LockExclusive()`, `LockUpgrade()`, `Unlock()` operations
- Enforce lock acquisition order to prevent deadlock
- Test transaction abort (all locks released) and commit
- See: [lab4_concurrency_control_en.md](docs/lab4_concurrency_control_en.md), [lock_manager.h](src/include/onebase/concurrency/lock_manager.h)

## When Adding New Code

### Adding an Executor
1. Create `src/execution/executors/my_executor.cpp` with header in [include/onebase/execution/executors/](src/include/onebase/execution/executors/)
2. Extend `AbstractExecutor`; implement `Init()`, `Next()`, `GetOutputSchema()`
3. Register in `ExecutorFactory::CreateExecutor()` ([executor_factory.cpp](src/execution/executors/executor_factory.cpp))
4. Add to `src/execution/executors/CMakeLists.txt`
5. Write test in `test/execution/executor_test.cpp` using `TEST(ExecutorTest, MyExecutor)`

### Adding a Test
1. Unit tests use `TEST(suite, name)` in `test/*/` folders
2. Graded evaluation tests use `GRADED_TEST(suite, name, points)` macro from [grading.h](test/eval/grading.h)
3. Add new test file to `test/CMakeLists.txt` via `onebase_add_test()` or `onebase_eval_test()`
4. Run with `ctest --test-dir build --output-on-failure`

### Adding a Component
1. Create source in appropriate `src/` directory (e.g., `src/buffer/`, `src/storage/`)
2. Add public header to [src/include/onebase/](src/include/onebase/) directory (mirror structure)
3. Link in `src/CMakeLists.txt` to `onebase` target
4. Document in relevant lab documentation file

## Configuration & Constants

From [config.h](src/include/onebase/common/config.h):
- `PAGE_SIZE = 4096` bytes (fixed)
- `DEFAULT_BUFFER_POOL_SIZE = 64` pages (can be overridden)
- `DEFAULT_LRU_K = 10` accesses (for LRU-K policy)
- `MAX_VARCHAR_SIZE = 256` characters

## Dependency Rules

- ✅ Execution layer imports Storage & Catalog (read-only)
- ✅ Storage layer imports Buffer (via PageGuard)
- ❌ Storage never imports Execution (avoids circular dependency)
- ✅ All layers import Common utilities
- ✅ Concurrency (LockManager) is side-by-side, accessed by all layers

## Error Handling

- Use exception-based error handling (not error codes)
- Throw `NotImplementedException` for stub methods that need implementation
- Use `ExceptionType` enum for categorized errors
- Example: `throw NotImplementedException("BufferPoolManager::NewPage")`

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Build fails immediately | Run `conda deactivate` (conda can pollute CMake paths) |
| Tests crash with "Unknown test `lab1_eval_test`" | Run `cmake ..` to regenerate build (test registration happens at configure time) |
| AddressSanitizer errors | Read carefully: line number shows memory operation, not always allocation site |
| Slow build | Increase parallel jobs: `cmake --build . -j8` (or use `nproc` for all cores) |
| "Unpin without fetch" error | Every `FetchPage*()` must have matching `UnpinPage()` or use PageGuard |

---

**Need lab-specific help?** Refer to [docs/](docs/) for detailed specifications, pseudocode, and diagrams in English and Chinese.

**Need to check common mistakes?** See [docs/common_mistakes.md](docs/common_mistakes.md).
