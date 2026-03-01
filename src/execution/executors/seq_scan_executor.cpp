#include "onebase/execution/executors/seq_scan_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // TODO(student): Initialize the sequential scan
  // - Get the table from catalog using plan_->GetTableOid()
  // - Set up iterator to table_heap->Begin()
  throw NotImplementedException("SeqScanExecutor::Init");
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return the next tuple from the table
  // - Advance iterator, skip tuples that don't match predicate
  // - Return false when no more tuples
  throw NotImplementedException("SeqScanExecutor::Next");
}

}  // namespace onebase
