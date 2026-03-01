#include "onebase/concurrency/transaction_manager.h"

namespace onebase {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *raw = txn.get();
  std::scoped_lock lock(txn_map_latch_);
  txn_map_[txn_id] = std::move(txn);
  return raw;
}

void TransactionManager::Commit(Transaction *txn) {
  txn->SetState(TransactionState::COMMITTED);
  // Release all locks
  auto *shared_set = txn->GetSharedLockSet();
  auto *exclusive_set = txn->GetExclusiveLockSet();
  for (const auto &rid : *shared_set) {
    lock_manager_->Unlock(txn, rid);
  }
  for (const auto &rid : *exclusive_set) {
    lock_manager_->Unlock(txn, rid);
  }
  shared_set->clear();
  exclusive_set->clear();
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);
  // Release all locks
  auto *shared_set = txn->GetSharedLockSet();
  auto *exclusive_set = txn->GetExclusiveLockSet();
  for (const auto &rid : *shared_set) {
    lock_manager_->Unlock(txn, rid);
  }
  for (const auto &rid : *exclusive_set) {
    lock_manager_->Unlock(txn, rid);
  }
  shared_set->clear();
  exclusive_set->clear();
}

}  // namespace onebase
