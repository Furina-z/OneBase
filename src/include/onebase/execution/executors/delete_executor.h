#pragma once
#include <memory>
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"

namespace onebase {

class DeleteExecutor : public AbstractExecutor {
 public:
  DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> child_executor);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  const DeletePlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool has_deleted_{false};
};

}  // namespace onebase
