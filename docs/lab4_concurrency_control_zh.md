# Lab 4: 并发控制 (Concurrency Control)

## 1. 实验概述

本实验的目标是为 OneBase 数据库实现一个 **锁管理器 (Lock Manager)**，它是并发控制的核心组件。锁管理器基于 **两阶段锁协议 (Two-Phase Locking, 2PL)**，为事务提供行级别的共享锁 (Shared Lock) 和排他锁 (Exclusive Lock)，确保并发事务的可串行化执行。

你将实现以下文件：

- **Lock Manager** (`src/concurrency/lock_manager.cpp`) — 4 个方法

## 2. 背景知识

### 2.1 两阶段锁协议 (2PL)

2PL 将事务分为两个阶段：

```
         GROWING          SHRINKING
        (加锁阶段)         (解锁阶段)
     ┌──────────────┐  ┌──────────────┐
     │ 可以获取新锁  │  │ 可以释放锁    │
     │ 不能释放锁    │  │ 不能获取新锁  │
     └──────┬───────┘  └──────┬───────┘
            │                 │
  BEGIN ────┤                 ├──── COMMIT/ABORT
            │  第一次 Unlock  │
            └────────────────┘
                 转换点
```

**事务状态转换：**
```
  GROWING ──(Unlock)──▶ SHRINKING ──▶ COMMITTED / ABORTED
```

- **GROWING 阶段**：事务可以请求任何锁，但不能释放锁
- **SHRINKING 阶段**：一旦事务释放了任何一个锁（调用 Unlock），就进入 SHRINKING 阶段，之后不能再获取任何新锁

### 2.2 锁兼容性矩阵

| 已持有 \ 请求 | SHARED | EXCLUSIVE |
|--------------|--------|-----------|
| (无锁)       | ✓ 授予 | ✓ 授予     |
| SHARED       | ✓ 授予 | ✗ 等待     |
| EXCLUSIVE    | ✗ 等待 | ✗ 等待     |

- 多个事务可以**同时**持有同一资源的 **SHARED 锁**
- **EXCLUSIVE 锁**与任何其他锁互斥
- 等待的事务会阻塞在**条件变量 (condition variable)** 上

### 2.3 锁升级 (Lock Upgrade)

如果一个事务已经持有某资源的 SHARED 锁，可以通过 `LockUpgrade` 将其升级为 EXCLUSIVE 锁：

```
S Lock ──(Upgrade)──▶ X Lock

条件：自己必须是该资源的唯一持有者（其他 SHARED 锁已全部释放）
限制：同一资源同一时间只允许一个事务在等待升级（upgrading_ 标志）
```

### 2.4 锁管理器数据结构

```cpp
struct LockRequest {
    txn_id_t txn_id_;       // 发起请求的事务 ID
    LockMode lock_mode_;    // SHARED 或 EXCLUSIVE
    bool granted_{false};   // 是否已被授予
};

struct LockRequestQueue {
    std::list<LockRequest> request_queue_;  // 该资源的所有锁请求
    std::condition_variable cv_;            // 等待队列的条件变量
    bool upgrading_{false};                 // 是否有事务正在等待升级
};

std::mutex latch_;                                      // 全局互斥锁
std::unordered_map<RID, LockRequestQueue> lock_table_;  // RID → 锁请求队列
```

### 2.5 条件变量 (Condition Variable) 用法

```cpp
std::unique_lock<std::mutex> lock(latch_);
// 等待直到条件满足
queue.cv_.wait(lock, [&]() {
    return /* 条件：例如没有排他锁持有者 */;
});
// 条件满足后继续执行
```

`cv_.wait()` 会：
1. 自动释放 `latch_`（让其他事务可以操作）
2. 阻塞当前线程
3. 当其他线程调用 `cv_.notify_all()` 时被唤醒
4. 重新获取 `latch_`，检查条件是否满足
5. 如果条件不满足，回到步骤 1 继续等待

## 3. 你的任务

**文件：** `src/concurrency/lock_manager.cpp`

| 方法 | 说明 | 难度 |
|------|------|------|
| `LockShared(txn, rid)` | 为事务获取 RID 上的共享锁 | ★★★ |
| `LockExclusive(txn, rid)` | 为事务获取 RID 上的排他锁 | ★★★ |
| `LockUpgrade(txn, rid)` | 将事务的共享锁升级为排他锁 | ★★★★ |
| `Unlock(txn, rid)` | 释放事务在 RID 上的锁 | ★★★ |

## 4. 实现指南

### 4.1 LockShared(Transaction *txn, const RID &rid)

```
1. 检查事务状态：
   if txn->GetState() == SHRINKING:
     txn->SetState(ABORTED)
     return false
   if txn->GetState() == ABORTED:
     return false

2. 检查是否已持有锁：
   if txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid):
     return true   // 已经持有，直接返回

3. 获取 lock_table_[rid] 的锁请求队列（自动创建）
4. 向队列中添加一个 SHARED 请求（未授予）:
   queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED)

5. 在条件变量上等待，直到可以授予：
   queue.cv_.wait(lock, [&]() {
     // 检查队列中是否有已授予的 EXCLUSIVE 锁
     // 如果没有，就可以授予 SHARED 锁
     for (auto &req : queue.request_queue_):
       if req.granted_ && req.lock_mode_ == EXCLUSIVE:
         return false   // 有排他锁，继续等待
     return true
   })

6. 检查等待期间事务是否被中止：
   if txn->GetState() == ABORTED:
     // 从队列中移除请求
     return false

7. 将请求标记为 granted
8. 将 rid 加入事务的 shared_lock_set_
9. return true
```

### 4.2 LockExclusive(Transaction *txn, const RID &rid)

```
1. 检查事务状态（同 LockShared）

2. 检查是否已持有排他锁：
   if txn->IsExclusiveLocked(rid):
     return true

3. 获取锁请求队列
4. 添加 EXCLUSIVE 请求

5. 在条件变量上等待：
   queue.cv_.wait(lock, [&]() {
     // 检查队列中是否有任何其他已授予的请求
     for (auto &req : queue.request_queue_):
       if req.granted_ && req.txn_id_ != txn->GetTransactionId():
         return false   // 有其他事务持有锁，等待
     return true
   })

6. 检查中止（同上）
7. 标记为 granted
8. 将 rid 加入事务的 exclusive_lock_set_
9. return true
```

### 4.3 LockUpgrade(Transaction *txn, const RID &rid)

```
1. 检查事务状态

2. 检查是否有其他事务正在升级同一资源：
   if queue.upgrading_:
     txn->SetState(ABORTED)
     return false

3. 设置 queue.upgrading_ = true

4. 找到当前事务在队列中的 SHARED 请求，将其 lock_mode_ 改为 EXCLUSIVE

5. 在条件变量上等待：
   queue.cv_.wait(lock, [&]() {
     // 检查是否自己是唯一持有者
     for (auto &req : queue.request_queue_):
       if req.granted_ && req.txn_id_ != txn->GetTransactionId():
         return false   // 还有其他锁持有者
     return true
   })

6. queue.upgrading_ = false
7. 检查中止
8. 标记为 granted
9. 从 shared_lock_set_ 移除 rid，加入 exclusive_lock_set_
10. return true
```

### 4.4 Unlock(Transaction *txn, const RID &rid)

```
1. 获取锁请求队列

2. 找到并移除当前事务的请求：
   for (auto it = queue.begin(); it != queue.end(); ++it):
     if it->txn_id_ == txn->GetTransactionId():
       queue.erase(it)
       break

3. 如果事务在 GROWING 阶段：
   txn->SetState(TransactionState::SHRINKING)

4. 更新事务的锁集合：
   txn->GetSharedLockSet()->erase(rid)
   txn->GetExclusiveLockSet()->erase(rid)

5. 通知所有等待的事务：
   queue.cv_.notify_all()

6. return true
```

## 5. 编译与测试

```bash
# 编译项目
cd build && cmake --build . -j$(nproc)

# 运行 Lab 4 相关测试
ctest --test-dir build -R lock_manager_test --output-on-failure

# 运行全部测试
ctest --test-dir build --output-on-failure
```

### 测试场景示例

测试通常会创建多个线程，每个线程代表一个事务：

```
线程 1 (Txn1): LockShared(rid)    ← 立即获得
线程 2 (Txn2): LockShared(rid)    ← 立即获得（与 S 锁兼容）
线程 3 (Txn3): LockExclusive(rid) ← 阻塞（有 S 锁）
线程 1: Unlock(rid)               ← Txn3 还在等（Txn2 还持有 S 锁）
线程 2: Unlock(rid)               ← Txn3 被唤醒，获得 X 锁
```

## 6. 常见错误

1. **死锁风险**：注意 `cv_.wait()` 需要使用 `std::unique_lock`（而非 `std::lock_guard`），因为 wait 需要临时释放锁。全局只用一把 `latch_` 可以避免锁排序问题
2. **2PL 状态检查**：在 LockShared/LockExclusive 中必须检查事务不在 SHRINKING 状态。如果在 SHRINKING 阶段请求锁，应该设置事务为 ABORTED 并返回 false
3. **升级冲突**：同一资源同一时间只能有一个事务在等待升级。如果 `upgrading_` 已为 true，新的升级请求应该 abort 该事务
4. **notify_all vs notify_one**：Unlock 后应该使用 `cv_.notify_all()` 而不是 `notify_one()`，因为可能有多个 SHARED 请求都可以被授予
5. **忘记 Unlock 中的状态转换**：当 GROWING 阶段的事务执行 Unlock 时，必须将其状态切换为 SHRINKING
6. **请求队列中的自己**：在等待条件判断中，注意排除自身事务 ID（特别是 LockUpgrade 和 LockExclusive）
7. **事务被中止后的清理**：如果等待过程中事务被外部中止（如死锁检测），需要从请求队列中移除该请求

## 7. 评分标准

| 组件 | 分值 |
|------|------|
| LockShared | 25 分 |
| LockExclusive | 25 分 |
| LockUpgrade | 25 分 |
| Unlock | 25 分 |
| **总计** | **100 分** |
