#include "onebase/optimizer/optimizer.h"

#include <memory>

#include "onebase/execution/expressions/column_value_expression.h"
#include "onebase/execution/expressions/comparison_expression.h"
#include "onebase/execution/plans/plan_nodes.h"

namespace onebase {

auto Optimizer::Optimize(AbstractPlanNodeRef plan) -> AbstractPlanNodeRef {
  // Recursively optimize children first
  for (uint32_t i = 0; i < plan->GetChildren().size(); i++) {
    plan->SetChildAt(i, Optimize(plan->GetChildAt(i)));
  }

  // Apply optimization rules
  plan = OptimizeNLJToHashJoin(plan);

  return plan;
}

auto Optimizer::OptimizeNLJToHashJoin(AbstractPlanNodeRef plan) -> AbstractPlanNodeRef {
  if (plan->GetType() != PlanType::NESTED_LOOP_JOIN) {
    return plan;
  }

  auto *nlj = dynamic_cast<NestedLoopJoinPlanNode *>(plan.get());
  if (nlj == nullptr) {
    return plan;
  }

  const auto &predicate = nlj->GetPredicate();
  if (predicate == nullptr) {
    return plan;
  }

  // Check if predicate is a simple equality comparison between columns
  auto *cmp = dynamic_cast<ComparisonExpression *>(predicate.get());
  if (cmp == nullptr || cmp->GetComparisonType() != ComparisonType::Equal) {
    return plan;
  }

  // Check both sides are column value expressions
  auto *left_col = dynamic_cast<ColumnValueExpression *>(cmp->GetChildAt(0).get());
  auto *right_col = dynamic_cast<ColumnValueExpression *>(cmp->GetChildAt(1).get());

  if (left_col == nullptr || right_col == nullptr) {
    return plan;
  }

  // Determine which side refers to left table (tuple_idx=0) and right table (tuple_idx=1)
  AbstractExpressionRef left_key_expr;
  AbstractExpressionRef right_key_expr;

  if (left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1) {
    // left.col = right.col — standard order
    left_key_expr = std::make_shared<ColumnValueExpression>(0, left_col->GetColIdx(), left_col->GetReturnType());
    right_key_expr = std::make_shared<ColumnValueExpression>(0, right_col->GetColIdx(), right_col->GetReturnType());
  } else if (left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0) {
    // right.col = left.col — swap
    left_key_expr = std::make_shared<ColumnValueExpression>(0, right_col->GetColIdx(), right_col->GetReturnType());
    right_key_expr = std::make_shared<ColumnValueExpression>(0, left_col->GetColIdx(), left_col->GetReturnType());
  } else {
    // Both refer to the same table — not a join condition
    return plan;
  }

  return std::make_shared<HashJoinPlanNode>(
      nlj->GetOutputSchema(),
      nlj->GetLeftPlan(),
      nlj->GetRightPlan(),
      left_key_expr,
      right_key_expr);
}

}  // namespace onebase
