#include "onebase/execution/executors/sort_executor.h"
#include <algorithm>
#include "onebase/common/exception.h"

namespace onebase {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  // TODO(student): Materialize all tuples from child, then sort
  // - Scan all child tuples into sorted_tuples_
  // - Sort using order_by expressions and directions
  // - Reset cursor_ to 0
  throw NotImplementedException("SortExecutor::Init");
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TODO(student): Return next sorted tuple
  throw NotImplementedException("SortExecutor::Next");
}

}  // namespace onebase
