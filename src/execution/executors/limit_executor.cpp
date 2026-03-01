#include "onebase/execution/executors/limit_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                              std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  // TODO(student): Initialize child executor and reset count
  throw NotImplementedException("LimitExecutor::Init");
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next tuple if count < limit, else false
  throw NotImplementedException("LimitExecutor::Next");
}

}  // namespace onebase
