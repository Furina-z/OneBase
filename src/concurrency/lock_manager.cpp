#include "onebase/concurrency/lock_manager.h"

#include <algorithm>

#include "onebase/common/exception.h"

namespace onebase {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsSharedLocked(rid) || txn->IsExclusiveLocked(rid)) {
    return true;
  }

  auto &queue = lock_table_[rid];
  txn_id_t txn_id = txn->GetTransactionId();

  queue.request_queue_.emplace_back(txn_id, LockMode::SHARED);

  auto request_it = std::prev(queue.request_queue_.end());

  queue.cv_.wait(lock, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }

    for (const auto &request : queue.request_queue_) {
      if (request.granted_ && request.lock_mode_ == LockMode::EXCLUSIVE &&
          request.txn_id_ != txn_id) {
        return false;
      }
    }

    return true;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    queue.request_queue_.erase(request_it);
    queue.cv_.notify_all();
    return false;
  }

  request_it->granted_ = true;
  txn->GetSharedLockSet()->insert(rid);

  return true;
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  if (txn->IsSharedLocked(rid)) {
    lock.unlock();
    return LockUpgrade(txn, rid);
  }

  auto &queue = lock_table_[rid];
  txn_id_t txn_id = txn->GetTransactionId();

  queue.request_queue_.emplace_back(txn_id, LockMode::EXCLUSIVE);

  auto request_it = std::prev(queue.request_queue_.end());

  queue.cv_.wait(lock, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }

    for (const auto &request : queue.request_queue_) {
      if (request.granted_ && request.txn_id_ != txn_id) {
        return false;
      }
    }

    return true;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    queue.request_queue_.erase(request_it);
    queue.cv_.notify_all();
    return false;
  }

  request_it->granted_ = true;
  txn->GetExclusiveLockSet()->insert(rid);

  return true;
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  if (!txn->IsSharedLocked(rid)) {
    return false;
  }

  auto &queue = lock_table_[rid];

  if (queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  queue.upgrading_ = true;

  txn_id_t txn_id = txn->GetTransactionId();

  auto request_it = queue.request_queue_.end();
  for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
    if (it->txn_id_ == txn_id && it->granted_ && it->lock_mode_ == LockMode::SHARED) {
      request_it = it;
      break;
    }
  }

  if (request_it == queue.request_queue_.end()) {
    queue.upgrading_ = false;
    queue.cv_.notify_all();
    return false;
  }

  request_it->lock_mode_ = LockMode::EXCLUSIVE;

  queue.cv_.wait(lock, [&]() {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }

    for (const auto &request : queue.request_queue_) {
      if (request.granted_ && request.txn_id_ != txn_id) {
        return false;
      }
    }

    return true;
  });

  queue.upgrading_ = false;

  if (txn->GetState() == TransactionState::ABORTED) {
    queue.request_queue_.erase(request_it);
    txn->GetSharedLockSet()->erase(rid);
    queue.cv_.notify_all();
    return false;
  }

  request_it->granted_ = true;
  request_it->lock_mode_ = LockMode::EXCLUSIVE;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->insert(rid);

  queue.cv_.notify_all();
  return true;
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  auto table_it = lock_table_.find(rid);
  if (table_it == lock_table_.end()) {
    return false;
  }

  auto &queue = table_it->second;
  txn_id_t txn_id = txn->GetTransactionId();

  bool found = false;

  for (auto it = queue.request_queue_.begin(); it != queue.request_queue_.end(); ++it) {
    if (it->txn_id_ == txn_id) {
      queue.request_queue_.erase(it);
      found = true;
      break;
    }
  }

  if (!found) {
    return false;
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  queue.cv_.notify_all();

  return true;
}

}  // namespace onebase