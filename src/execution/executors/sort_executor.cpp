#include "onebase/execution/executors/sort_executor.h"

#include <algorithm>

#include "onebase/common/exception.h"

namespace onebase {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  cursor_ = 0;

  Tuple tuple;
  RID rid;

  while (child_executor_->Next(&tuple, &rid)) {
    tuple.SetRID(rid);
    sorted_tuples_.push_back(tuple);
  }

  const Schema &child_schema = child_executor_->GetOutputSchema();

  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
            [&](const Tuple &a, const Tuple &b) {
              for (const auto &order_by : plan_->GetOrderBys()) {
                bool is_ascending = order_by.first;
                const auto &expr = order_by.second;

                Value av = expr->Evaluate(&a, &child_schema);
                Value bv = expr->Evaluate(&b, &child_schema);

                if (av.IsNull() && bv.IsNull()) {
                  continue;
                }

                if (av.IsNull()) {
                  return is_ascending;
                }

                if (bv.IsNull()) {
                  return !is_ascending;
                }

                bool less = av.CompareLessThan(bv).GetAsBoolean();
                bool greater = av.CompareGreaterThan(bv).GetAsBoolean();

                if (less) {
                  return is_ascending;
                }

                if (greater) {
                  return !is_ascending;
                }
              }

              return false;
            });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= sorted_tuples_.size()) {
    return false;
  }

  *tuple = sorted_tuples_[cursor_];
  *rid = tuple->GetRID();

  cursor_++;
  return true;
}

}  // namespace onebase