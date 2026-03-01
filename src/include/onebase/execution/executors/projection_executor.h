#pragma once
#include <memory>
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"

namespace onebase {

class ProjectionExecutor : public AbstractExecutor {
 public:
  ProjectionExecutor(ExecutorContext *exec_ctx, const ProjectionPlanNode *plan,
                     std::unique_ptr<AbstractExecutor> child_executor);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  const ProjectionPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
};

}  // namespace onebase
