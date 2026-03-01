#include "onebase/execution/executors/hash_join_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                    std::unique_ptr<AbstractExecutor> left_executor,
                                    std::unique_ptr<AbstractExecutor> right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void HashJoinExecutor::Init() {
  // TODO(student): Build hash table from left child, initialize right child
  throw NotImplementedException("HashJoinExecutor::Init");
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Probe hash table with right child tuples
  // - Phase 1 (in Init): Build hash table from left child on left_key_expr
  // - Phase 2 (in Next): For each right tuple, probe hash table using right_key_expr
  throw NotImplementedException("HashJoinExecutor::Next");
}

}  // namespace onebase
