#pragma once
#include <memory>
#include <string>
#include "onebase/buffer/buffer_pool_manager.h"
#include "onebase/catalog/catalog.h"
#include "onebase/concurrency/lock_manager.h"
#include "onebase/concurrency/transaction_manager.h"
#include "onebase/execution/execution_engine.h"
#include "onebase/execution/executor_context.h"
#include "onebase/storage/disk/disk_manager.h"

namespace onebase {

class OneBaseInstance {
 public:
  explicit OneBaseInstance(const std::string &db_file_name,
                           size_t buffer_pool_size = DEFAULT_BUFFER_POOL_SIZE);
  ~OneBaseInstance() = default;

  auto GetCatalog() -> Catalog * { return catalog_.get(); }
  auto GetBufferPoolManager() -> BufferPoolManager * { return bpm_.get(); }
  auto GetTransactionManager() -> TransactionManager * { return txn_mgr_.get(); }
  auto GetLockManager() -> LockManager * { return lock_mgr_.get(); }
  auto GetExecutionEngine() -> ExecutionEngine * { return execution_engine_.get(); }

 private:
  std::unique_ptr<DiskManager> disk_mgr_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<LockManager> lock_mgr_;
  std::unique_ptr<TransactionManager> txn_mgr_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ExecutorContext> exec_ctx_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
};

}  // namespace onebase
