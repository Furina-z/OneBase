#include "onebase/execution/executor_factory.h"
#include "onebase/execution/executors/aggregation_executor.h"
#include "onebase/execution/executors/delete_executor.h"
#include "onebase/execution/executors/hash_join_executor.h"
#include "onebase/execution/executors/index_scan_executor.h"
#include "onebase/execution/executors/insert_executor.h"
#include "onebase/execution/executors/limit_executor.h"
#include "onebase/execution/executors/nested_loop_join_executor.h"
#include "onebase/execution/executors/seq_scan_executor.h"
#include "onebase/execution/executors/sort_executor.h"
#include "onebase/execution/executors/update_executor.h"
#include "onebase/execution/plans/plan_nodes.h"
#include <stdexcept>

namespace onebase {

auto ExecutorFactory::CreateExecutor(ExecutorContext *exec_ctx, const AbstractPlanNodeRef &plan)
    -> std::unique_ptr<AbstractExecutor> {
  switch (plan->GetType()) {
    case PlanType::SEQ_SCAN:
      return std::make_unique<SeqScanExecutor>(
          exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));

    case PlanType::INDEX_SCAN:
      return std::make_unique<IndexScanExecutor>(
          exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));

    case PlanType::INSERT: {
      auto *insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child));
    }

    case PlanType::DELETE: {
      auto *delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child));
    }

    case PlanType::UPDATE: {
      auto *update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child));
    }

    case PlanType::NESTED_LOOP_JOIN: {
      auto *nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode *>(plan.get());
      auto left = CreateExecutor(exec_ctx, nlj_plan->GetLeftPlan());
      auto right = CreateExecutor(exec_ctx, nlj_plan->GetRightPlan());
      return std::make_unique<NestedLoopJoinExecutor>(exec_ctx, nlj_plan, std::move(left), std::move(right));
    }

    case PlanType::HASH_JOIN: {
      auto *hj_plan = dynamic_cast<const HashJoinPlanNode *>(plan.get());
      auto left = CreateExecutor(exec_ctx, hj_plan->GetLeftPlan());
      auto right = CreateExecutor(exec_ctx, hj_plan->GetRightPlan());
      return std::make_unique<HashJoinExecutor>(exec_ctx, hj_plan, std::move(left), std::move(right));
    }

    case PlanType::AGGREGATION: {
      auto *agg_plan = dynamic_cast<const AggregationPlanNode *>(plan.get());
      auto child = CreateExecutor(exec_ctx, agg_plan->GetChildPlan());
      return std::make_unique<AggregationExecutor>(exec_ctx, agg_plan, std::move(child));
    }

    case PlanType::SORT: {
      auto *sort_plan = dynamic_cast<const SortPlanNode *>(plan.get());
      auto child = CreateExecutor(exec_ctx, sort_plan->GetChildPlan());
      return std::make_unique<SortExecutor>(exec_ctx, sort_plan, std::move(child));
    }

    case PlanType::LIMIT: {
      auto *limit_plan = dynamic_cast<const LimitPlanNode *>(plan.get());
      auto child = CreateExecutor(exec_ctx, limit_plan->GetChildPlan());
      return std::make_unique<LimitExecutor>(exec_ctx, limit_plan, std::move(child));
    }

    default:
      throw std::runtime_error("Unknown plan type in ExecutorFactory");
  }
}

}  // namespace onebase
