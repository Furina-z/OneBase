#include "onebase/execution/executors/aggregation_executor.h"

#include <string>
#include <unordered_map>

#include "onebase/common/exception.h"

namespace onebase {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                          std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

namespace {

auto MakeGroupKey(const std::vector<Value> &values) -> std::string {
  std::string key;
  for (const auto &value : values) {
    key += value.ToString();
    key += "#";
  }
  return key;
}

struct AggState {
  std::vector<Value> group_values_;
  std::vector<Value> agg_values_;
  std::vector<bool> initialized_;
};

}  // namespace

void AggregationExecutor::Init() {
  child_executor_->Init();
  result_tuples_.clear();
  cursor_ = 0;

  const Schema &child_schema = child_executor_->GetOutputSchema();

  const auto &group_bys = plan_->GetGroupBys();
  const auto &aggregates = plan_->GetAggregates();
  const auto &agg_types = plan_->GetAggregateTypes();

  std::unordered_map<std::string, AggState> groups;

  Tuple child_tuple;
  RID child_rid;
  bool saw_input = false;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    saw_input = true;

    std::vector<Value> group_values;
    group_values.reserve(group_bys.size());

    for (const auto &group_expr : group_bys) {
      group_values.push_back(group_expr->Evaluate(&child_tuple, &child_schema));
    }

    std::string group_key = MakeGroupKey(group_values);

    auto it = groups.find(group_key);
    if (it == groups.end()) {
      AggState state;
      state.group_values_ = group_values;
      state.agg_values_.reserve(agg_types.size());
      state.initialized_.assign(agg_types.size(), false);

      for (size_t i = 0; i < agg_types.size(); ++i) {
        switch (agg_types[i]) {
          case AggregationType::CountStarAggregate:
          case AggregationType::CountAggregate:
            state.agg_values_.push_back(Value(TypeId::INTEGER, 0));
            state.initialized_[i] = true;
            break;
          case AggregationType::SumAggregate:
          case AggregationType::MinAggregate:
          case AggregationType::MaxAggregate:
            state.agg_values_.push_back(Value(TypeId::INTEGER));
            break;
        }
      }

      it = groups.emplace(group_key, std::move(state)).first;
    }

    AggState &state = it->second;

    for (size_t i = 0; i < agg_types.size(); ++i) {
      switch (agg_types[i]) {
        case AggregationType::CountStarAggregate: {
          int32_t count = state.agg_values_[i].GetAsInteger();
          state.agg_values_[i] = Value(TypeId::INTEGER, count + 1);
          break;
        }

        case AggregationType::CountAggregate: {
          Value input = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (!input.IsNull()) {
            int32_t count = state.agg_values_[i].GetAsInteger();
            state.agg_values_[i] = Value(TypeId::INTEGER, count + 1);
          }
          break;
        }

        case AggregationType::SumAggregate: {
          Value input = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (input.IsNull()) {
            break;
          }

          if (!state.initialized_[i]) {
            state.agg_values_[i] = input;
            state.initialized_[i] = true;
          } else {
            state.agg_values_[i] = state.agg_values_[i].Add(input);
          }
          break;
        }

        case AggregationType::MinAggregate: {
          Value input = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (input.IsNull()) {
            break;
          }

          if (!state.initialized_[i]) {
            state.agg_values_[i] = input;
            state.initialized_[i] = true;
          } else if (input.CompareLessThan(state.agg_values_[i]).GetAsBoolean()) {
            state.agg_values_[i] = input;
          }
          break;
        }

        case AggregationType::MaxAggregate: {
          Value input = aggregates[i]->Evaluate(&child_tuple, &child_schema);
          if (input.IsNull()) {
            break;
          }

          if (!state.initialized_[i]) {
            state.agg_values_[i] = input;
            state.initialized_[i] = true;
          } else if (input.CompareGreaterThan(state.agg_values_[i]).GetAsBoolean()) {
            state.agg_values_[i] = input;
          }
          break;
        }
      }
    }
  }

  if (!saw_input && group_bys.empty()) {
    std::vector<Value> values;
    values.reserve(agg_types.size());

    for (const auto &agg_type : agg_types) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
        case AggregationType::CountAggregate:
          values.push_back(Value(TypeId::INTEGER, 0));
          break;
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          values.push_back(Value(TypeId::INTEGER));
          break;
      }
    }

    result_tuples_.emplace_back(std::move(values));
    return;
  }

  for (auto &entry : groups) {
    AggState &state = entry.second;

    std::vector<Value> values;
    values.reserve(state.group_values_.size() + state.agg_values_.size());

    for (const auto &value : state.group_values_) {
      values.push_back(value);
    }

    for (const auto &value : state.agg_values_) {
      values.push_back(value);
    }

    result_tuples_.emplace_back(std::move(values));
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }

  *tuple = result_tuples_[cursor_];
  *rid = RID(INVALID_PAGE_ID, 0);

  cursor_++;
  return true;
}

}  // namespace onebase