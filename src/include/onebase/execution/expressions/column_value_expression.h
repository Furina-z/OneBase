#pragma once
#include "onebase/execution/expressions/abstract_expression.h"

namespace onebase {

class ColumnValueExpression : public AbstractExpression {
 public:
  ColumnValueExpression(uint32_t tuple_idx, uint32_t col_idx, TypeId ret_type)
      : AbstractExpression({}, ret_type), tuple_idx_(tuple_idx), col_idx_(col_idx) {}

  auto Evaluate(const Tuple *tuple, const Schema *schema) const -> Value override {
    return tuple->GetValue(schema, col_idx_);
  }

  auto EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
                    const Tuple *right_tuple, const Schema *right_schema) const -> Value override {
    return tuple_idx_ == 0 ? left_tuple->GetValue(left_schema, col_idx_)
                           : right_tuple->GetValue(right_schema, col_idx_);
  }

  auto GetTupleIdx() const -> uint32_t { return tuple_idx_; }
  auto GetColIdx() const -> uint32_t { return col_idx_; }

 private:
  uint32_t tuple_idx_;
  uint32_t col_idx_;
};

}  // namespace onebase
