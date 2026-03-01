#pragma once
#include "onebase/catalog/schema.h"
#include "onebase/execution/executor_context.h"
#include "onebase/execution/plans/abstract_plan_node.h"
#include "onebase/storage/table/tuple.h"

namespace onebase {

class AbstractExecutor {
 public:
  explicit AbstractExecutor(ExecutorContext *exec_ctx) : exec_ctx_(exec_ctx) {}
  virtual ~AbstractExecutor() = default;

  virtual void Init() = 0;
  virtual auto Next(Tuple *tuple, RID *rid) -> bool = 0;
  virtual auto GetOutputSchema() const -> const Schema & = 0;

 protected:
  auto GetExecutorContext() -> ExecutorContext * { return exec_ctx_; }

 private:
  ExecutorContext *exec_ctx_;
};

}  // namespace onebase
