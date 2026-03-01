#include "onebase/execution/executors/delete_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // TODO(student): Initialize child executor
  throw NotImplementedException("DeleteExecutor::Init");
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Delete tuples identified by child executor
  // - Get tuples from child, delete from table_heap
  // - Update any indexes
  // - Return count of deleted rows
  throw NotImplementedException("DeleteExecutor::Next");
}

}  // namespace onebase
