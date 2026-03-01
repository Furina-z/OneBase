#pragma once
#include <memory>
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"

namespace onebase {

class LimitExecutor : public AbstractExecutor {
 public:
  LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                std::unique_ptr<AbstractExecutor> child_executor);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  const LimitPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  size_t count_{0};
};

}  // namespace onebase
