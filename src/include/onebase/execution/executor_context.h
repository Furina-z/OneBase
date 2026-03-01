#pragma once
#include "onebase/catalog/catalog.h"
#include "onebase/buffer/buffer_pool_manager.h"

namespace onebase {

class Transaction;  // forward declaration

class ExecutorContext {
 public:
  ExecutorContext(Catalog *catalog, BufferPoolManager *bpm, Transaction *txn = nullptr)
      : catalog_(catalog), bpm_(bpm), txn_(txn) {}

  auto GetCatalog() const -> Catalog * { return catalog_; }
  auto GetBufferPoolManager() const -> BufferPoolManager * { return bpm_; }
  auto GetTransaction() const -> Transaction * { return txn_; }

 private:
  Catalog *catalog_;
  BufferPoolManager *bpm_;
  Transaction *txn_;
};

}  // namespace onebase
