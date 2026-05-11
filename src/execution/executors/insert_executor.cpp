#include "onebase/execution/executors/insert_executor.h"

#include "onebase/common/exception.h"

namespace onebase {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  has_inserted_ = false;
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }

  has_inserted_ = true;

  auto *catalog = GetExecutorContext()->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  const Schema &table_schema = table_info->schema_;
  const Schema &child_schema = child_executor_->GetOutputSchema();
  auto indexes = catalog->GetTableIndexes(table_info->name_);

  int32_t insert_count = 0;

  Tuple child_tuple;
  RID child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> values;
    values.reserve(table_schema.GetColumnCount());

    for (uint32_t i = 0; i < table_schema.GetColumnCount(); ++i) {
      values.push_back(child_tuple.GetValue(&child_schema, i));
    }

    Tuple insert_tuple(std::move(values));
    auto inserted_rid = table_info->table_->InsertTuple(insert_tuple);

    if (!inserted_rid.has_value()) {
      continue;
    }

    insert_tuple.SetRID(inserted_rid.value());

    for (auto *index_info : indexes) {
      if (!index_info->key_attrs_.empty()) {
        uint32_t key_attr = index_info->key_attrs_[0];
        int32_t key = insert_tuple.GetValue(&table_schema, key_attr).GetAsInteger();
        index_info->InsertEntry(key, inserted_rid.value());
      }
    }

    insert_count++;
  }

  std::vector<Value> output_values{Value(TypeId::INTEGER, insert_count)};
  *tuple = Tuple(std::move(output_values));
  *rid = RID(INVALID_PAGE_ID, 0);

  return true;
}

}  // namespace onebase