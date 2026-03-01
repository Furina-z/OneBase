#pragma once
#include "onebase/execution/expressions/abstract_expression.h"

namespace onebase {

enum class LogicType { And, Or };

class LogicExpression : public AbstractExpression {
 public:
  LogicExpression(AbstractExpressionRef left, AbstractExpressionRef right, LogicType logic_type)
      : AbstractExpression({std::move(left), std::move(right)}, TypeId::BOOLEAN),
        logic_type_(logic_type) {}

  auto Evaluate(const Tuple *tuple, const Schema *schema) const -> Value override {
    auto left = GetChildAt(0)->Evaluate(tuple, schema);
    auto right = GetChildAt(1)->Evaluate(tuple, schema);
    return PerformLogic(left, right);
  }

  auto EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
                    const Tuple *right_tuple, const Schema *right_schema) const -> Value override {
    auto left = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    auto right = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    return PerformLogic(left, right);
  }

 private:
  auto PerformLogic(const Value &left, const Value &right) const -> Value {
    switch (logic_type_) {
      case LogicType::And: return left.LogicalAnd(right);
      case LogicType::Or:  return left.LogicalOr(right);
      default: return Value(TypeId::BOOLEAN, false);
    }
  }

  LogicType logic_type_;
};

}  // namespace onebase
