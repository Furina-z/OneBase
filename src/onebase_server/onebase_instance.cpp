#include "onebase/server/onebase_instance.h"

namespace onebase {

OneBaseInstance::OneBaseInstance(const std::string &db_file_name, size_t buffer_pool_size) {
  disk_mgr_ = std::make_unique<DiskManager>(db_file_name);
  bpm_ = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_mgr_.get());
  lock_mgr_ = std::make_unique<LockManager>();
  txn_mgr_ = std::make_unique<TransactionManager>(lock_mgr_.get());
  catalog_ = std::make_unique<Catalog>(bpm_.get());
  exec_ctx_ = std::make_unique<ExecutorContext>(catalog_.get(), bpm_.get());
  execution_engine_ = std::make_unique<ExecutionEngine>(exec_ctx_.get());
}

}  // namespace onebase
