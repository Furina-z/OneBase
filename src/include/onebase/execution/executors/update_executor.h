#pragma once
#include <memory>
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"

namespace onebase {

class UpdateExecutor : public AbstractExecutor {
 public:
  UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> child_executor);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  const UpdatePlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool has_updated_{false};
};

}  // namespace onebase
