#include "onebase/execution/executors/delete_executor.h"

#include "onebase/common/exception.h"

namespace onebase {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  has_deleted_ = false;
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }

  has_deleted_ = true;

  auto *catalog = GetExecutorContext()->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  const Schema &table_schema = table_info->schema_;
  const Schema &child_schema = child_executor_->GetOutputSchema();
  auto indexes = catalog->GetTableIndexes(table_info->name_);

  int32_t delete_count = 0;

  Tuple child_tuple;
  RID child_rid;

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    for (auto *index_info : indexes) {
      if (!index_info->key_attrs_.empty()) {
        uint32_t key_attr = index_info->key_attrs_[0];
        int32_t key = child_tuple.GetValue(&child_schema, key_attr).GetAsInteger();
        index_info->RemoveEntry(key, child_rid);
      }
    }

    table_info->table_->DeleteTuple(child_rid);
    delete_count++;
  }

  std::vector<Value> output_values{Value(TypeId::INTEGER, delete_count)};
  *tuple = Tuple(std::move(output_values));
  *rid = RID(INVALID_PAGE_ID, 0);

  return true;
}

}  // namespace onebase