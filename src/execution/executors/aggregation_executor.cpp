#include "onebase/execution/executors/aggregation_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                          std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  // TODO(student): Initialize child and build aggregation hash table
  // - Scan all tuples from child
  // - Group by group_by expressions
  // - Compute aggregates (COUNT, SUM, MIN, MAX) per group
  throw NotImplementedException("AggregationExecutor::Init");
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next aggregation result
  throw NotImplementedException("AggregationExecutor::Next");
}

}  // namespace onebase
