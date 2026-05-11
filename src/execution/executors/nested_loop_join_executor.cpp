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
  result_tuples_.clear();
  cursor_ = 0;

  left_executor_->Init();

  const Schema &left_schema = left_executor_->GetOutputSchema();
  const Schema &right_schema = right_executor_->GetOutputSchema();

  Tuple left_tuple;
  RID left_rid;

  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();

    Tuple right_tuple;
    RID right_rid;

    while (right_executor_->Next(&right_tuple, &right_rid)) {
      const auto &predicate = plan_->GetPredicate();

      if (predicate != nullptr) {
        Value pred_value = predicate->EvaluateJoin(&left_tuple, &left_schema, &right_tuple, &right_schema);

        if (pred_value.IsNull() || !pred_value.GetAsBoolean()) {
          continue;
        }
      }

      std::vector<Value> values;
      values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());

      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_schema, i));
      }

      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(right_tuple.GetValue(&right_schema, i));
      }

      result_tuples_.emplace_back(std::move(values));
    }
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }

  *tuple = result_tuples_[cursor_];
  *rid = RID(INVALID_PAGE_ID, 0);

  cursor_++;
  return true;
}

}  // namespace onebase