#pragma once
#include "onebase/catalog/catalog.h"
#include "onebase/execution/expressions/abstract_expression.h"
#include "onebase/execution/plans/abstract_plan_node.h"

namespace onebase {

class SeqScanPlanNode : public AbstractPlanNode {
 public:
  SeqScanPlanNode(Schema output_schema, table_oid_t table_oid,
                  AbstractExpressionRef predicate = nullptr)
      : AbstractPlanNode(std::move(output_schema), {}),
        table_oid_(table_oid), predicate_(std::move(predicate)) {}

  auto GetType() const -> PlanType override { return PlanType::SEQ_SCAN; }
  auto GetTableOid() const -> table_oid_t { return table_oid_; }
  auto GetPredicate() const -> const AbstractExpressionRef & { return predicate_; }

 private:
  table_oid_t table_oid_;
  AbstractExpressionRef predicate_;
};

class IndexScanPlanNode : public AbstractPlanNode {
 public:
  IndexScanPlanNode(Schema output_schema, table_oid_t table_oid, index_oid_t index_oid)
      : AbstractPlanNode(std::move(output_schema), {}),
        table_oid_(table_oid), index_oid_(index_oid) {}

  auto GetType() const -> PlanType override { return PlanType::INDEX_SCAN; }
  auto GetTableOid() const -> table_oid_t { return table_oid_; }
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

 private:
  table_oid_t table_oid_;
  index_oid_t index_oid_;
};

class InsertPlanNode : public AbstractPlanNode {
 public:
  InsertPlanNode(Schema output_schema, AbstractPlanNodeRef child, table_oid_t table_oid)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}),
        table_oid_(table_oid) {}

  auto GetType() const -> PlanType override { return PlanType::INSERT; }
  auto GetTableOid() const -> table_oid_t { return table_oid_; }
  auto GetChildPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }

 private:
  table_oid_t table_oid_;
};

class DeletePlanNode : public AbstractPlanNode {
 public:
  DeletePlanNode(Schema output_schema, AbstractPlanNodeRef child, table_oid_t table_oid)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}),
        table_oid_(table_oid) {}

  auto GetType() const -> PlanType override { return PlanType::DELETE; }
  auto GetTableOid() const -> table_oid_t { return table_oid_; }
  auto GetChildPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }

 private:
  table_oid_t table_oid_;
};

class UpdatePlanNode : public AbstractPlanNode {
 public:
  UpdatePlanNode(Schema output_schema, AbstractPlanNodeRef child, table_oid_t table_oid,
                 std::vector<AbstractExpressionRef> update_exprs)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}),
        table_oid_(table_oid), update_exprs_(std::move(update_exprs)) {}

  auto GetType() const -> PlanType override { return PlanType::UPDATE; }
  auto GetTableOid() const -> table_oid_t { return table_oid_; }
  auto GetChildPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }
  auto GetUpdateExpressions() const -> const std::vector<AbstractExpressionRef> & { return update_exprs_; }

 private:
  table_oid_t table_oid_;
  std::vector<AbstractExpressionRef> update_exprs_;
};

class NestedLoopJoinPlanNode : public AbstractPlanNode {
 public:
  NestedLoopJoinPlanNode(Schema output_schema, AbstractPlanNodeRef left, AbstractPlanNodeRef right,
                          AbstractExpressionRef predicate)
      : AbstractPlanNode(std::move(output_schema), {std::move(left), std::move(right)}),
        predicate_(std::move(predicate)) {}

  auto GetType() const -> PlanType override { return PlanType::NESTED_LOOP_JOIN; }
  auto GetLeftPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }
  auto GetRightPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(1); }
  auto GetPredicate() const -> const AbstractExpressionRef & { return predicate_; }

 private:
  AbstractExpressionRef predicate_;
};

class HashJoinPlanNode : public AbstractPlanNode {
 public:
  HashJoinPlanNode(Schema output_schema, AbstractPlanNodeRef left, AbstractPlanNodeRef right,
                   AbstractExpressionRef left_key_expr, AbstractExpressionRef right_key_expr)
      : AbstractPlanNode(std::move(output_schema), {std::move(left), std::move(right)}),
        left_key_expr_(std::move(left_key_expr)), right_key_expr_(std::move(right_key_expr)) {}

  auto GetType() const -> PlanType override { return PlanType::HASH_JOIN; }
  auto GetLeftPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }
  auto GetRightPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(1); }
  auto GetLeftKeyExpression() const -> const AbstractExpressionRef & { return left_key_expr_; }
  auto GetRightKeyExpression() const -> const AbstractExpressionRef & { return right_key_expr_; }

 private:
  AbstractExpressionRef left_key_expr_;
  AbstractExpressionRef right_key_expr_;
};

enum class AggregationType { CountStarAggregate, CountAggregate, SumAggregate, MinAggregate, MaxAggregate };

class AggregationPlanNode : public AbstractPlanNode {
 public:
  AggregationPlanNode(Schema output_schema, AbstractPlanNodeRef child,
                      std::vector<AbstractExpressionRef> group_bys,
                      std::vector<AbstractExpressionRef> aggregates,
                      std::vector<AggregationType> agg_types)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}),
        group_bys_(std::move(group_bys)), aggregates_(std::move(aggregates)),
        agg_types_(std::move(agg_types)) {}

  auto GetType() const -> PlanType override { return PlanType::AGGREGATION; }
  auto GetChildPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }
  auto GetGroupBys() const -> const std::vector<AbstractExpressionRef> & { return group_bys_; }
  auto GetAggregates() const -> const std::vector<AbstractExpressionRef> & { return aggregates_; }
  auto GetAggregateTypes() const -> const std::vector<AggregationType> & { return agg_types_; }

 private:
  std::vector<AbstractExpressionRef> group_bys_;
  std::vector<AbstractExpressionRef> aggregates_;
  std::vector<AggregationType> agg_types_;
};

class SortPlanNode : public AbstractPlanNode {
 public:
  using OrderByType = std::pair<bool, AbstractExpressionRef>;  // {is_ascending, expr}

  SortPlanNode(Schema output_schema, AbstractPlanNodeRef child,
               std::vector<OrderByType> order_bys)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}),
        order_bys_(std::move(order_bys)) {}

  auto GetType() const -> PlanType override { return PlanType::SORT; }
  auto GetChildPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }
  auto GetOrderBys() const -> const std::vector<OrderByType> & { return order_bys_; }

 private:
  std::vector<OrderByType> order_bys_;
};

class LimitPlanNode : public AbstractPlanNode {
 public:
  LimitPlanNode(Schema output_schema, AbstractPlanNodeRef child, size_t limit)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}),
        limit_(limit) {}

  auto GetType() const -> PlanType override { return PlanType::LIMIT; }
  auto GetChildPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }
  auto GetLimit() const -> size_t { return limit_; }

 private:
  size_t limit_;
};

class ProjectionPlanNode : public AbstractPlanNode {
 public:
  ProjectionPlanNode(Schema output_schema, AbstractPlanNodeRef child,
                     std::vector<AbstractExpressionRef> expressions)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}),
        expressions_(std::move(expressions)) {}

  auto GetType() const -> PlanType override { return PlanType::PROJECTION; }
  auto GetChildPlan() const -> const AbstractPlanNodeRef & { return GetChildAt(0); }
  auto GetExpressions() const -> const std::vector<AbstractExpressionRef> & { return expressions_; }

 private:
  std::vector<AbstractExpressionRef> expressions_;
};

}  // namespace onebase
