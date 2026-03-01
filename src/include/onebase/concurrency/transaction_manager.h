#pragma once
#include <atomic>
#include <memory>
#include <unordered_map>
#include <mutex>
#include "onebase/concurrency/lock_manager.h"
#include "onebase/concurrency/transaction.h"

namespace onebase {

class TransactionManager {
 public:
  explicit TransactionManager(LockManager *lock_manager) : lock_manager_(lock_manager) {}

  auto Begin(IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ) -> Transaction *;
  void Commit(Transaction *txn);
  void Abort(Transaction *txn);

 private:
  std::atomic<txn_id_t> next_txn_id_{0};
  LockManager *lock_manager_;
  std::mutex txn_map_latch_;
  std::unordered_map<txn_id_t, std::unique_ptr<Transaction>> txn_map_;
};

}  // namespace onebase
