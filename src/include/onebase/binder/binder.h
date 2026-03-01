#pragma once

#include <string>
#include <vector>

#include "onebase/catalog/catalog.h"
#include "onebase/execution/plans/abstract_plan_node.h"

namespace onebase {

class Binder {
 public:
  explicit Binder(Catalog *catalog);

  auto BindQuery(const std::string &sql) -> AbstractPlanNodeRef;

 private:
  Catalog *catalog_;
};

}  // namespace onebase
