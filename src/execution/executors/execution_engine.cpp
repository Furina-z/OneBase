#include "onebase/execution/execution_engine.h"

namespace onebase {

auto ExecutionEngine::Execute(const AbstractPlanNodeRef &plan, std::vector<Tuple> *result_set) -> bool {
  auto executor = ExecutorFactory::CreateExecutor(exec_ctx_, plan);
  executor->Init();
  Tuple tuple;
  RID rid;
  while (executor->Next(&tuple, &rid)) {
    if (result_set != nullptr) {
      result_set->push_back(tuple);
    }
  }
  return true;
}

}  // namespace onebase
