#pragma once
#include "onebase/execution/expressions/abstract_expression.h"

namespace onebase {

enum class ComparisonType { Equal, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual };

class ComparisonExpression : public AbstractExpression {
 public:
  ComparisonExpression(AbstractExpressionRef left, AbstractExpressionRef right, ComparisonType comp_type)
      : AbstractExpression({std::move(left), std::move(right)}, TypeId::BOOLEAN),
        comp_type_(comp_type) {}

  auto Evaluate(const Tuple *tuple, const Schema *schema) const -> Value override {
    auto left = GetChildAt(0)->Evaluate(tuple, schema);
    auto right = GetChildAt(1)->Evaluate(tuple, schema);
    return PerformComparison(left, right);
  }

  auto EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
                    const Tuple *right_tuple, const Schema *right_schema) const -> Value override {
    auto left = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    auto right = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
    return PerformComparison(left, right);
  }

  auto GetComparisonType() const -> ComparisonType { return comp_type_; }

 private:
  auto PerformComparison(const Value &left, const Value &right) const -> Value {
    switch (comp_type_) {
      case ComparisonType::Equal:              return left.CompareEquals(right);
      case ComparisonType::NotEqual:           return left.CompareNotEquals(right);
      case ComparisonType::LessThan:           return left.CompareLessThan(right);
      case ComparisonType::LessThanOrEqual:    return left.CompareLessThanOrEqual(right);
      case ComparisonType::GreaterThan:        return left.CompareGreaterThan(right);
      case ComparisonType::GreaterThanOrEqual: return left.CompareGreaterThanOrEqual(right);
      default: return Value(TypeId::BOOLEAN, false);
    }
  }

  ComparisonType comp_type_;
};

}  // namespace onebase
