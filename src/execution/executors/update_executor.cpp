#include "onebase/execution/executors/update_executor.h"

#include "onebase/common/exception.h"

namespace onebase {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  has_updated_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    return false;
  }

  has_updated_ = true;

  auto *catalog = GetExecutorContext()->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  const Schema &table_schema = table_info->schema_;
  const Schema &child_schema = child_executor_->GetOutputSchema();
  auto indexes = catalog->GetTableIndexes(table_info->name_);

  int32_t update_count = 0;

  Tuple old_tuple;
  RID old_rid;

  while (child_executor_->Next(&old_tuple, &old_rid)) {
    std::vector<Value> new_values;
    new_values.reserve(plan_->GetUpdateExpressions().size());

    for (const auto &expr : plan_->GetUpdateExpressions()) {
      new_values.push_back(expr->Evaluate(&old_tuple, &child_schema));
    }

    Tuple new_tuple(std::move(new_values));
    new_tuple.SetRID(old_rid);

    for (auto *index_info : indexes) {
      if (!index_info->key_attrs_.empty()) {
        uint32_t key_attr = index_info->key_attrs_[0];
        int32_t old_key = old_tuple.GetValue(&child_schema, key_attr).GetAsInteger();
        index_info->RemoveEntry(old_key, old_rid);
      }
    }

    bool success = table_info->table_->UpdateTuple(old_rid, new_tuple);

    if (success) {
      for (auto *index_info : indexes) {
        if (!index_info->key_attrs_.empty()) {
          uint32_t key_attr = index_info->key_attrs_[0];
          int32_t new_key = new_tuple.GetValue(&table_schema, key_attr).GetAsInteger();
          index_info->InsertEntry(new_key, old_rid);
        }
      }

      update_count++;
    }
  }

  std::vector<Value> output_values{Value(TypeId::INTEGER, update_count)};
  *tuple = Tuple(std::move(output_values));
  *rid = RID(INVALID_PAGE_ID, 0);

  return true;
}

}  // namespace onebase