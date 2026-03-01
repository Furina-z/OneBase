#include "onebase/execution/executors/index_scan_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // TODO(student): Initialize index scan using the B+ tree index
  throw NotImplementedException("IndexScanExecutor::Init");
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next tuple from index scan
  throw NotImplementedException("IndexScanExecutor::Next");
}

}  // namespace onebase
