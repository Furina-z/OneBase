#pragma once
#include "onebase/execution/expressions/abstract_expression.h"

namespace onebase {

enum class ArithmeticType { Plus, Minus, Multiply, Divide, Modulo };

class ArithmeticExpression : public AbstractExpression {
 public:
  ArithmeticExpression(AbstractExpressionRef left, AbstractExpressionRef right,
                       ArithmeticType arith_type)
      : AbstractExpression({std::move(left), std::move(right)}, left ? left->GetReturnType() : TypeId::INTEGER),
        arith_type_(arith_type) {}

  auto Evaluate(const Tuple *tuple, const Schema *schema) const -> Value override {
    auto left = GetChildAt(0)->Evaluate(tuple, schema);
    auto right = GetChildAt(1)->Evaluate(tuple, schema);
    return PerformComputation(left, right);
  }

  auto EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
                    const Tuple *right_tuple, const Schema *right_schema) const -> Value override {
    auto left = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    auto right = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    return PerformComputation(left, right);
  }

 private:
  auto PerformComputation(const Value &left, const Value &right) const -> Value {
    switch (arith_type_) {
      case ArithmeticType::Plus:     return left.Add(right);
      case ArithmeticType::Minus:    return left.Subtract(right);
      case ArithmeticType::Multiply: return left.Multiply(right);
      case ArithmeticType::Divide:   return left.Divide(right);
      case ArithmeticType::Modulo:   return left.Modulo(right);
      default: return Value();
    }
  }

  ArithmeticType arith_type_;
};

}  // namespace onebase
