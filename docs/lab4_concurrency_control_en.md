# Lab 4: Concurrency Control

## 1. Overview

The goal of this lab is to implement a **Lock Manager** for the OneBase database, which is the core component of concurrency control. The Lock Manager is based on the **Two-Phase Locking (2PL)** protocol, providing row-level Shared Locks and Exclusive Locks for transactions to ensure serializable execution of concurrent transactions.

You will implement the following file:

- **Lock Manager** (`src/concurrency/lock_manager.cpp`) — 4 methods

## 2. Background

### 2.1 Two-Phase Locking Protocol (2PL)

2PL divides a transaction into two phases:

```
         GROWING          SHRINKING
      (Locking Phase)   (Unlocking Phase)
     ┌──────────────┐  ┌──────────────┐
     │ Can acquire   │  │ Can release   │
     │ new locks     │  │ locks         │
     │ Cannot release│  │ Cannot acquire│
     │ locks         │  │ new locks     │
     └──────┬───────┘  └──────┬───────┘
            │                 │
  BEGIN ────┤                 ├──── COMMIT/ABORT
            │  First Unlock   │
            └────────────────┘
              Transition Point
```

**Transaction state transitions:**
```
  GROWING ──(Unlock)──▶ SHRINKING ──▶ COMMITTED / ABORTED
```

- **GROWING phase**: The transaction can request any lock but cannot release locks
- **SHRINKING phase**: Once the transaction releases any lock (calls Unlock), it enters the SHRINKING phase and cannot acquire any new locks afterward

### 2.2 Lock Compatibility Matrix

| Held \ Requested | SHARED | EXCLUSIVE |
|------------------|--------|-----------|
| (No lock)        | ✓ Grant | ✓ Grant  |
| SHARED           | ✓ Grant | ✗ Wait   |
| EXCLUSIVE        | ✗ Wait  | ✗ Wait   |

- Multiple transactions can **simultaneously** hold **SHARED locks** on the same resource
- An **EXCLUSIVE lock** is mutually exclusive with any other lock
- Waiting transactions block on a **condition variable**

### 2.3 Lock Upgrade

If a transaction already holds a SHARED lock on a resource, it can upgrade it to an EXCLUSIVE lock via `LockUpgrade`:

```
S Lock ──(Upgrade)──▶ X Lock

Condition: The transaction must be the only holder of the resource (all other SHARED locks have been released)
Restriction: Only one transaction can be waiting for an upgrade on a resource at a time (upgrading_ flag)
```

### 2.4 Lock Manager Data Structures

```cpp
struct LockRequest {
    txn_id_t txn_id_;       // Transaction ID of the requester
    LockMode lock_mode_;    // SHARED or EXCLUSIVE
    bool granted_{false};   // Whether the lock has been granted
};

struct LockRequestQueue {
    std::list<LockRequest> request_queue_;  // All lock requests for this resource
    std::condition_variable cv_;            // Condition variable for the wait queue
    bool upgrading_{false};                 // Whether a transaction is waiting for upgrade
};

std::mutex latch_;                                      // Global mutex
std::unordered_map<RID, LockRequestQueue> lock_table_;  // RID → lock request queue
```

### 2.5 Condition Variable Usage

```cpp
std::unique_lock<std::mutex> lock(latch_);
// Wait until condition is met
queue.cv_.wait(lock, [&]() {
    return /* condition: e.g., no exclusive lock holder */;
});
// Continue execution after condition is satisfied
```

`cv_.wait()` will:
1. Automatically release `latch_` (allowing other transactions to operate)
2. Block the current thread
3. Wake up when another thread calls `cv_.notify_all()`
4. Re-acquire `latch_` and check whether the condition is satisfied
5. If the condition is not satisfied, go back to step 1 and continue waiting

## 3. Your Tasks

**File:** `src/concurrency/lock_manager.cpp`

| Method | Description | Difficulty |
|--------|-------------|------------|
| `LockShared(txn, rid)` | Acquire a shared lock on the RID for the transaction | ★★★ |
| `LockExclusive(txn, rid)` | Acquire an exclusive lock on the RID for the transaction | ★★★ |
| `LockUpgrade(txn, rid)` | Upgrade the transaction's shared lock to an exclusive lock | ★★★★ |
| `Unlock(txn, rid)` | Release the transaction's lock on the RID | ★★★ |

## 4. Implementation Guide

### 4.1 LockShared(Transaction *txn, const RID &rid)

```
1. Check transaction state:
   if txn->GetState() == SHRINKING:
     txn->SetState(ABORTED)
     return false
   if txn->GetState() == ABORTED:
     return false

2. Check if the lock is already held:
   if txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid):
     return true   // Already held, return directly

3. Get the lock request queue for lock_table_[rid] (auto-created)
4. Add a SHARED request (not granted) to the queue:
   queue.request_queue_.emplace_back(txn->GetTransactionId(), LockMode::SHARED)

5. Wait on the condition variable until the lock can be granted:
   queue.cv_.wait(lock, [&]() {
     // Check if there is a granted EXCLUSIVE lock in the queue
     // If not, the SHARED lock can be granted
     for (auto &req : queue.request_queue_):
       if req.granted_ && req.lock_mode_ == EXCLUSIVE:
         return false   // Exclusive lock exists, keep waiting
     return true
   })

6. Check if the transaction was aborted while waiting:
   if txn->GetState() == ABORTED:
     // Remove the request from the queue
     return false

7. Mark the request as granted
8. Add rid to the transaction's shared_lock_set_
9. return true
```

### 4.2 LockExclusive(Transaction *txn, const RID &rid)

```
1. Check transaction state (same as LockShared)

2. Check if an exclusive lock is already held:
   if txn->IsExclusiveLocked(rid):
     return true

3. Get the lock request queue
4. Add an EXCLUSIVE request

5. Wait on the condition variable:
   queue.cv_.wait(lock, [&]() {
     // Check if there are any other granted requests in the queue
     for (auto &req : queue.request_queue_):
       if req.granted_ && req.txn_id_ != txn->GetTransactionId():
         return false   // Another transaction holds a lock, wait
     return true
   })

6. Check for abort (same as above)
7. Mark as granted
8. Add rid to the transaction's exclusive_lock_set_
9. return true
```

### 4.3 LockUpgrade(Transaction *txn, const RID &rid)

```
1. Check transaction state

2. Check if another transaction is already upgrading on the same resource:
   if queue.upgrading_:
     txn->SetState(ABORTED)
     return false

3. Set queue.upgrading_ = true

4. Find the current transaction's SHARED request in the queue and change its lock_mode_ to EXCLUSIVE

5. Wait on the condition variable:
   queue.cv_.wait(lock, [&]() {
     // Check if this transaction is the only holder
     for (auto &req : queue.request_queue_):
       if req.granted_ && req.txn_id_ != txn->GetTransactionId():
         return false   // Other lock holders still exist
     return true
   })

6. queue.upgrading_ = false
7. Check for abort
8. Mark as granted
9. Remove rid from shared_lock_set_, add to exclusive_lock_set_
10. return true
```

### 4.4 Unlock(Transaction *txn, const RID &rid)

```
1. Get the lock request queue

2. Find and remove the current transaction's request:
   for (auto it = queue.begin(); it != queue.end(); ++it):
     if it->txn_id_ == txn->GetTransactionId():
       queue.erase(it)
       break

3. If the transaction is in the GROWING phase:
   txn->SetState(TransactionState::SHRINKING)

4. Update the transaction's lock sets:
   txn->GetSharedLockSet()->erase(rid)
   txn->GetExclusiveLockSet()->erase(rid)

5. Notify all waiting transactions:
   queue.cv_.notify_all()

6. return true
```

## 5. Build & Test

```bash
# Build the project
cd build && cmake --build . -j$(nproc)

# Run Lab 4 related tests
ctest --test-dir build -R lock_manager_test --output-on-failure

# Run all tests
ctest --test-dir build --output-on-failure
```

### Test Scenario Example

Tests typically create multiple threads, each representing a transaction:

```
Thread 1 (Txn1): LockShared(rid)    ← Granted immediately
Thread 2 (Txn2): LockShared(rid)    ← Granted immediately (compatible with S lock)
Thread 3 (Txn3): LockExclusive(rid) ← Blocked (S lock held)
Thread 1: Unlock(rid)               ← Txn3 still waiting (Txn2 still holds S lock)
Thread 2: Unlock(rid)               ← Txn3 wakes up, acquires X lock
```

## 6. Common Mistakes

1. **Deadlock risk**: Note that `cv_.wait()` requires `std::unique_lock` (not `std::lock_guard`), because wait needs to temporarily release the lock. Using a single global `latch_` avoids lock ordering issues.
2. **2PL state check**: In LockShared/LockExclusive, you must check that the transaction is not in the SHRINKING state. If a lock is requested during the SHRINKING phase, set the transaction to ABORTED and return false.
3. **Upgrade conflict**: Only one transaction can be waiting for an upgrade on the same resource at a time. If `upgrading_` is already true, the new upgrade request should abort that transaction.
4. **notify_all vs notify_one**: After Unlock, use `cv_.notify_all()` instead of `notify_one()`, because multiple SHARED requests may all be grantable.
5. **Forgetting state transition in Unlock**: When a transaction in the GROWING phase executes Unlock, its state must be changed to SHRINKING.
6. **Self in request queue**: In wait condition checks, make sure to exclude your own transaction ID (especially in LockUpgrade and LockExclusive).
7. **Cleanup after transaction abort**: If a transaction is externally aborted while waiting (e.g., by deadlock detection), the request must be removed from the request queue.

## 7. Grading

| Component | Points |
|-----------|--------|
| LockShared | 25 |
| LockExclusive | 25 |
| LockUpgrade | 25 |
| Unlock | 25 |
| **Total** | **100** |
