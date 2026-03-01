#include "onebase/execution/executors/insert_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // TODO(student): Initialize child executor
  throw NotImplementedException("InsertExecutor::Init");
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Insert tuples from child into the table
  // - Get tuples from child, insert into table_heap
  // - Update any indexes
  // - Return count of inserted rows as a single integer tuple
  throw NotImplementedException("InsertExecutor::Next");
}

}  // namespace onebase
