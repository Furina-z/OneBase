#pragma once
#include <atomic>
#include <unordered_set>
#include "onebase/common/rid.h"
#include "onebase/common/types.h"

namespace onebase {

enum class TransactionState { GROWING, SHRINKING, COMMITTED, ABORTED };
enum class IsolationLevel { READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ };

class Transaction {
 public:
  explicit Transaction(txn_id_t txn_id, IsolationLevel isolation_level = IsolationLevel::REPEATABLE_READ)
      : txn_id_(txn_id), isolation_level_(isolation_level) {}

  auto GetTransactionId() const -> txn_id_t { return txn_id_; }
  auto GetIsolationLevel() const -> IsolationLevel { return isolation_level_; }
  auto GetState() const -> TransactionState { return state_; }
  void SetState(TransactionState state) { state_ = state; }

  auto GetSharedLockSet() -> std::unordered_set<RID> * { return &shared_lock_set_; }
  auto GetExclusiveLockSet() -> std::unordered_set<RID> * { return &exclusive_lock_set_; }

  auto IsSharedLocked(const RID &rid) const -> bool {
    return shared_lock_set_.count(rid) > 0;
  }
  auto IsExclusiveLocked(const RID &rid) const -> bool {
    return exclusive_lock_set_.count(rid) > 0;
  }

 private:
  txn_id_t txn_id_;
  IsolationLevel isolation_level_;
  TransactionState state_{TransactionState::GROWING};
  std::unordered_set<RID> shared_lock_set_;
  std::unordered_set<RID> exclusive_lock_set_;
};

}  // namespace onebase
