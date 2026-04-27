#include "onebase/execution/executors/utility_executor.h"

#include <algorithm>
#include <stdexcept>

#include "onebase/common/exception.h"

namespace onebase {

namespace {

auto JoinStrings(const std::vector<std::string> &parts, const std::string &separator) -> std::string {
  std::string joined;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i > 0) {
      joined += separator;
    }
    joined += parts[i];
  }
  return joined;
}

}  // namespace

UtilityExecutor::UtilityExecutor(ExecutorContext *exec_ctx, const UtilityPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

auto UtilityExecutor::MakeCommandRow(const std::string &tag) -> void {
  result_rows_.emplace_back(std::vector<Value>{Value(TypeId::VARCHAR, tag)});
}

auto UtilityExecutor::MakeTableRow(const std::vector<std::string> &values) -> void {
  std::vector<Value> row_values;
  row_values.reserve(values.size());
  for (const auto &value : values) {
    row_values.emplace_back(TypeId::VARCHAR, value);
  }
  result_rows_.emplace_back(std::move(row_values));
}

void UtilityExecutor::Init() {
  result_rows_.clear();
  cursor_ = 0;

  auto *catalog = GetExecutorContext()->GetCatalog();

  switch (plan_->GetUtilityType()) {
    case UtilityType::CREATE_INDEX: {
      auto *index_info = catalog->CreateIndex(plan_->GetIndexName(), plan_->GetTableName(), plan_->GetKeyAttrs());
      if (index_info == nullptr) {
        throw std::runtime_error("Failed to create index: " + plan_->GetIndexName());
      }
      MakeCommandRow("CREATE INDEX " + plan_->GetIndexName());
      return;
    }

    case UtilityType::DROP_INDEX: {
      bool removed_any = false;
      for (const auto &index_name : plan_->GetObjectNames()) {
        std::string matched_table_name;
        for (const auto *table_info : catalog->GetAllTables()) {
          if (catalog->GetIndex(index_name, table_info->name_) != nullptr) {
            if (!matched_table_name.empty()) {
              throw std::runtime_error("Ambiguous index name: " + index_name);
            }
            matched_table_name = table_info->name_;
          }
        }

        if (matched_table_name.empty()) {
          if (!plan_->GetMissingOk()) {
            throw std::runtime_error("Index not found: " + index_name);
          }
          continue;
        }

        removed_any = catalog->DropIndex(index_name, matched_table_name) || removed_any;
      }

      MakeCommandRow(removed_any ? "DROP INDEX" : "DROP INDEX (0 rows)");
      return;
    }

    case UtilityType::SHOW_TABLES: {
      auto tables = catalog->GetAllTables();
      std::sort(tables.begin(), tables.end(), [](const TableInfo *lhs, const TableInfo *rhs) {
        return lhs->name_ < rhs->name_;
      });
      for (const auto *table_info : tables) {
        MakeTableRow({table_info->name_});
      }
      return;
    }

    case UtilityType::SHOW_INDEXES: {
      auto indexes = catalog->GetAllIndexes();
      std::sort(indexes.begin(), indexes.end(), [](const IndexInfo *lhs, const IndexInfo *rhs) {
        if (lhs->name_ != rhs->name_) {
          return lhs->name_ < rhs->name_;
        }
        if (lhs->table_name_ != rhs->table_name_) {
          return lhs->table_name_ < rhs->table_name_;
        }
        return lhs->oid_ < rhs->oid_;
      });
      for (const auto *index_info : indexes) {
        std::vector<std::string> columns;
        for (const auto &column : index_info->key_schema_.GetColumns()) {
          columns.push_back(column.GetName());
        }
        MakeTableRow({index_info->name_, index_info->table_name_, JoinStrings(columns, ",")});
      }
      return;
    }

    case UtilityType::SHOW_SCHEMA: {
      auto *table_info = catalog->GetTable(plan_->GetTableName());
      if (table_info == nullptr) {
        throw std::runtime_error("Table not found: " + plan_->GetTableName());
      }
      const auto &schema = table_info->schema_;
      for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
        MakeTableRow({schema.GetColumn(i).GetName(), schema.GetColumn(i).ToString()});
      }
      return;
    }
  }

  throw std::runtime_error("Unsupported utility plan type");
}

auto UtilityExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= result_rows_.size()) {
    return false;
  }
  *tuple = result_rows_[cursor_++];
  if (rid != nullptr) {
    *rid = RID{};
  }
  return true;
}

}  // namespace onebase