#include "onebase/execution/executors/update_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  // TODO(student): Initialize child executor
  throw NotImplementedException("UpdateExecutor::Init");
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Update tuples using update expressions
  // - Get tuples from child, evaluate update expressions, update table_heap
  // - Return count of updated rows
  throw NotImplementedException("UpdateExecutor::Next");
}

}  // namespace onebase
