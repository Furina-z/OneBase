#pragma once

#include <vector>

#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"

namespace onebase {

class UtilityExecutor : public AbstractExecutor {
 public:
  UtilityExecutor(ExecutorContext *exec_ctx, const UtilityPlanNode *plan);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  auto MakeCommandRow(const std::string &tag) -> void;
  auto MakeTableRow(const std::vector<std::string> &values) -> void;

  const UtilityPlanNode *plan_;
  std::vector<Tuple> result_rows_;
  size_t cursor_{0};
};

}  // namespace onebase