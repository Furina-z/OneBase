#pragma once
#include <memory>
#include "onebase/execution/executor_context.h"
#include "onebase/execution/executors/abstract_executor.h"
#include "onebase/execution/plans/abstract_plan_node.h"

namespace onebase {

class ExecutorFactory {
 public:
  static auto CreateExecutor(ExecutorContext *exec_ctx, const AbstractPlanNodeRef &plan)
      -> std::unique_ptr<AbstractExecutor>;
};

}  // namespace onebase
