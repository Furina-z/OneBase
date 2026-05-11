#include "onebase/execution/executors/index_scan_executor.h"

#include "onebase/common/exception.h"

namespace onebase {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto *catalog = GetExecutorContext()->GetCatalog();

  table_info_ = catalog->GetTable(plan_->GetTableOid());
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());

  matching_rids_.clear();
  cursor_ = 0;

  if (table_info_ == nullptr || index_info_ == nullptr) {
    return;
  }

  if (!index_info_->SupportsPointLookup()) {
    return;
  }

  Value lookup_value = plan_->GetLookupKey()->Evaluate(nullptr, nullptr);

  if (lookup_value.IsNull()) {
    return;
  }

  int32_t lookup_key = lookup_value.GetAsInteger();
  const auto *rids = index_info_->LookupInteger(lookup_key);

  if (rids != nullptr) {
    matching_rids_ = *rids;
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (cursor_ < matching_rids_.size()) {
    RID current_rid = matching_rids_[cursor_++];

    Tuple raw_tuple = table_info_->table_->GetTuple(current_rid);

    const auto &predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      Value pred_value = predicate->Evaluate(&raw_tuple, &table_info_->schema_);

      if (pred_value.IsNull() || !pred_value.GetAsBoolean()) {
        continue;
      }
    }

    std::vector<Value> values;
    values.reserve(table_info_->schema_.GetColumnCount());

    for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); ++i) {
      values.push_back(raw_tuple.GetValue(&table_info_->schema_, i));
    }

    Tuple materialized_tuple(std::move(values));
    materialized_tuple.SetRID(current_rid);

    *tuple = materialized_tuple;
    *rid = current_rid;

    return true;
  }

  return false;
}

}  // namespace onebase