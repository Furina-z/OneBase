#pragma once
#include "onebase/execution/expressions/abstract_expression.h"

namespace onebase {

class ConstantValueExpression : public AbstractExpression {
 public:
  explicit ConstantValueExpression(const Value &val)
      : AbstractExpression({}, val.GetTypeId()), val_(val) {}

  auto Evaluate([[maybe_unused]] const Tuple *tuple,
                [[maybe_unused]] const Schema *schema) const -> Value override {
    return val_;
  }

  auto EvaluateJoin([[maybe_unused]] const Tuple *left_tuple,
                    [[maybe_unused]] const Schema *left_schema,
                    [[maybe_unused]] const Tuple *right_tuple,
                    [[maybe_unused]] const Schema *right_schema) const -> Value override {
    return val_;
  }

  auto GetValue() const -> const Value & { return val_; }

 private:
  Value val_;
};

}  // namespace onebase
