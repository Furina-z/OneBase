#include "onebase/execution/executors/nested_loop_join_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx,
                                                const NestedLoopJoinPlanNode *plan,
                                                std::unique_ptr<AbstractExecutor> left_executor,
                                                std::unique_ptr<AbstractExecutor> right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  // TODO(student): Initialize both child executors
  throw NotImplementedException("NestedLoopJoinExecutor::Init");
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Perform nested loop join
  // - For each left tuple, scan all right tuples
  // - Evaluate predicate on (left, right) pairs
  // - Output matching combined tuples
  throw NotImplementedException("NestedLoopJoinExecutor::Next");
}

}  // namespace onebase
