#pragma once
#include <memory>
#include <vector>
#include "onebase/catalog/schema.h"

namespace onebase {

enum class PlanType {
  INVALID,
  SEQ_SCAN,
  INDEX_SCAN,
  INSERT,
  DELETE,
  UPDATE,
  NESTED_LOOP_JOIN,
  HASH_JOIN,
  AGGREGATION,
  SORT,
  LIMIT,
  PROJECTION,
  UTILITY,
};

class AbstractPlanNode {
 public:
  AbstractPlanNode(Schema output_schema, std::vector<std::shared_ptr<AbstractPlanNode>> children)
      : output_schema_(std::move(output_schema)), children_(std::move(children)) {}
  virtual ~AbstractPlanNode() = default;

  auto GetOutputSchema() const -> const Schema & { return output_schema_; }

  auto GetChildAt(uint32_t idx) const -> const std::shared_ptr<AbstractPlanNode> & {
    return children_[idx];
  }
  auto GetChildren() const -> const std::vector<std::shared_ptr<AbstractPlanNode>> & {
    return children_;
  }

  void SetChildAt(uint32_t idx, std::shared_ptr<AbstractPlanNode> child) {
    children_[idx] = std::move(child);
  }

  virtual auto GetType() const -> PlanType = 0;

 protected:
  Schema output_schema_;
  std::vector<std::shared_ptr<AbstractPlanNode>> children_;
};

using AbstractPlanNodeRef = std::shared_ptr<AbstractPlanNode>;

}  // namespace onebase
