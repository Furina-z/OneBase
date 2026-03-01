#include "onebase/concurrency/lock_manager.h"
#include "onebase/common/exception.h"

namespace onebase {

auto LockManager::LockShared(Transaction *txn, const RID &rid) -> bool {
  // TODO(student): Implement shared lock acquisition using 2PL
  // - Check transaction state (must be GROWING)
  // - Add lock request to queue
  // - Wait until granted (no exclusive locks held by others)
  // - Add to txn's shared lock set
  throw NotImplementedException("LockManager::LockShared");
}

auto LockManager::LockExclusive(Transaction *txn, const RID &rid) -> bool {
  // TODO(student): Implement exclusive lock acquisition using 2PL
  // - Check transaction state (must be GROWING)
  // - Add lock request to queue
  // - Wait until granted (no other locks held)
  // - Add to txn's exclusive lock set
  throw NotImplementedException("LockManager::LockExclusive");
}

auto LockManager::LockUpgrade(Transaction *txn, const RID &rid) -> bool {
  // TODO(student): Upgrade shared lock to exclusive
  // - Only one upgrade at a time per queue
  // - Wait until all other shared locks released
  throw NotImplementedException("LockManager::LockUpgrade");
}

auto LockManager::Unlock(Transaction *txn, const RID &rid) -> bool {
  // TODO(student): Release a lock
  // - Transition txn to SHRINKING state (2PL)
  // - Remove from request queue
  // - Notify waiting transactions
  throw NotImplementedException("LockManager::Unlock");
}

}  // namespace onebase
