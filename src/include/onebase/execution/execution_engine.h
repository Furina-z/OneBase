#pragma once
#include <vector>
#include "onebase/execution/executor_context.h"
#include "onebase/execution/executor_factory.h"
#include "onebase/execution/plans/abstract_plan_node.h"
#include "onebase/storage/table/tuple.h"

namespace onebase {

class ExecutionEngine {
 public:
  explicit ExecutionEngine(ExecutorContext *exec_ctx) : exec_ctx_(exec_ctx) {}

  auto Execute(const AbstractPlanNodeRef &plan, std::vector<Tuple> *result_set) -> bool;

 private:
  ExecutorContext *exec_ctx_;
};

}  // namespace onebase
