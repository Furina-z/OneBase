#pragma once
#include <memory>
#include <vector>
#include "onebase/catalog/schema.h"
#include "onebase/storage/table/tuple.h"
#include "onebase/type/value.h"

namespace onebase {

enum class ExpressionType {
  INVALID,
  COLUMN_VALUE,
  CONSTANT,
  COMPARISON,
  ARITHMETIC,
  LOGIC,
};

class AbstractExpression {
 public:
  AbstractExpression(std::vector<std::shared_ptr<AbstractExpression>> children, TypeId ret_type)
      : children_(std::move(children)), ret_type_(ret_type) {}
  virtual ~AbstractExpression() = default;

  virtual auto Evaluate(const Tuple *tuple, const Schema *schema) const -> Value = 0;
  virtual auto EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
                            const Tuple *right_tuple, const Schema *right_schema) const -> Value = 0;

  auto GetChildAt(uint32_t idx) const -> const std::shared_ptr<AbstractExpression> & {
    return children_[idx];
  }
  auto GetChildren() const -> const std::vector<std::shared_ptr<AbstractExpression>> & {
    return children_;
  }
  auto GetReturnType() const -> TypeId { return ret_type_; }

 protected:
  std::vector<std::shared_ptr<AbstractExpression>> children_;
  TypeId ret_type_;
};

using AbstractExpressionRef = std::shared_ptr<AbstractExpression>;

}  // namespace onebase
