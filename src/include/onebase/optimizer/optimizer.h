#pragma once

#include "onebase/execution/plans/abstract_plan_node.h"

namespace onebase {

class Optimizer {
 public:
  auto Optimize(AbstractPlanNodeRef plan) -> AbstractPlanNodeRef;

 private:
  auto OptimizeNLJToHashJoin(AbstractPlanNodeRef plan) -> AbstractPlanNodeRef;
};

}  // namespace onebase
