#pragma once
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/plan_nodes.h"
#include "onebase/storage/table/table_heap.h"

namespace onebase {

class SeqScanExecutor : public AbstractExecutor {
 public:
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);
  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->GetOutputSchema(); }

 private:
  const SeqScanPlanNode *plan_;
  TableInfo *table_info_{nullptr};
  TableHeap::Iterator iter_{nullptr, RID(INVALID_PAGE_ID, 0)};
  TableHeap::Iterator end_{nullptr, RID(INVALID_PAGE_ID, 0)};
};

}  // namespace onebase
