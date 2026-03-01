#include "onebase/execution/executors/projection_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

ProjectionExecutor::ProjectionExecutor(ExecutorContext *exec_ctx, const ProjectionPlanNode *plan,
                                        std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void ProjectionExecutor::Init() {
  // TODO(student): Initialize child executor
  throw NotImplementedException("ProjectionExecutor::Init");
}

auto ProjectionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Get next tuple from child, evaluate each expression in
  // plan_->GetExpressions() against it, and build output tuple from the results.
  throw NotImplementedException("ProjectionExecutor::Next");
}

}  // namespace onebase
