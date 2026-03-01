#pragma once
#include <memory>
#include <vector>
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"

namespace onebase {

class SortExecutor : public AbstractExecutor {
 public:
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
               std::unique_ptr<AbstractExecutor> child_executor);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> sorted_tuples_;
  size_t cursor_{0};
};

}  // namespace onebase
