#pragma once

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "onebase/binder/binder.h"
#include "onebase/catalog/catalog.h"
#include "onebase/catalog/schema.h"
#include "onebase/optimizer/optimizer.h"
#include "onebase/server/onebase_instance.h"
#include "onebase/storage/table/tuple.h"
#include "onebase/type/type_id.h"
#include "onebase/type/value.h"

namespace onebase::eval {

inline auto TrimCopy(std::string text) -> std::string {
  auto is_not_space = [](unsigned char ch) { return !std::isspace(ch); };
  text.erase(text.begin(), std::find_if(text.begin(), text.end(), is_not_space));
  text.erase(std::find_if(text.rbegin(), text.rend(), is_not_space).base(), text.end());
  return text;
}

inline auto StripTrailingSemicolon(std::string text) -> std::string {
  text = TrimCopy(std::move(text));
  while (!text.empty() && text.back() == ';') {
    text.pop_back();
    text = TrimCopy(std::move(text));
  }
  return text;
}

class Table {
 public:
  auto GetColumnCount() const -> size_t { return headers_.size(); }
  auto GetRowCount() const -> size_t { return rows_.size(); }
  auto GetHeader(size_t idx) const -> const std::string & { return headers_.at(idx); }
  auto GetRow(size_t idx) const -> const std::vector<std::string> & { return rows_.at(idx); }
  auto GetHeaders() const -> const std::vector<std::string> & { return headers_; }
  auto GetRows() const -> const std::vector<std::vector<std::string>> & { return rows_; }

  auto FindColumn(const std::string &name) const -> int {
    for (size_t i = 0; i < headers_.size(); ++i) {
      if (headers_[i] == name) {
        return static_cast<int>(i);
      }
    }
    return -1;
  }

 private:
  friend class SqlTestClient;
  std::vector<std::string> headers_;
  std::vector<std::vector<std::string>> rows_;
};

class SqlTestClient {
 public:
  explicit SqlTestClient(std::string test_name, size_t buffer_pool_size = 64)
      : db_name_("__sql_eval_" + TrimCopy(std::move(test_name)) + "_" +
                 std::to_string(reinterpret_cast<uintptr_t>(this)) + ".db"),
        instance_(std::make_unique<OneBaseInstance>(db_name_, buffer_pool_size)) {}

  SqlTestClient(const SqlTestClient &) = delete;
  auto operator=(const SqlTestClient &) -> SqlTestClient & = delete;

  ~SqlTestClient() {
    if (instance_ != nullptr && instance_->GetDiskManager() != nullptr) {
      instance_->GetDiskManager()->ShutDown();
    }
    instance_.reset();
    std::remove(db_name_.c_str());
  }

  auto GetCatalog() -> Catalog * { return instance_->GetCatalog(); }

  auto BindQuery(const std::string &sql) -> AbstractPlanNodeRef {
    Binder binder(GetCatalog());
    return binder.BindQuery(sql);
  }

  auto OptimizeQuery(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
    Optimizer optimizer;
    return optimizer.Optimize(plan);
  }

  auto ExecutePlan(const AbstractPlanNodeRef &plan) -> Table {
    std::vector<Tuple> tuples;
    instance_->GetExecutionEngine()->Execute(plan, &tuples);

    Table table;
    const auto &schema = plan->GetOutputSchema();
    for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
      table.headers_.push_back(schema.GetColumn(i).GetName());
    }
    for (const auto &tuple : tuples) {
      std::vector<std::string> row;
      row.reserve(schema.GetColumnCount());
      for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
        row.push_back(tuple.GetValue(i).ToString());
      }
      table.rows_.push_back(std::move(row));
    }
    return table;
  }

  auto CreateTable(const std::string &table_name, const Schema &schema) -> TableInfo * {
    return GetCatalog()->CreateTable(table_name, schema);
  }

  auto SeedTable(const std::string &table_name,
                 std::initializer_list<std::vector<Value>> rows) -> void {
    std::vector<std::vector<Value>> copied_rows(rows.begin(), rows.end());
    SeedTable(table_name, copied_rows);
  }

  auto SeedTable(const std::string &table_name, const std::vector<std::vector<Value>> &rows) -> void {
    auto *table = GetCatalog()->GetTable(table_name);
    if (table == nullptr) {
      throw std::runtime_error("Table not found: " + table_name);
    }
    for (const auto &row_values : rows) {
      auto inserted = table->table_->InsertTuple(Tuple(row_values));
      if (!inserted.has_value()) {
        throw std::runtime_error("Failed to seed table: " + table_name);
      }
    }
  }

  auto ExecuteQuery(const std::string &sql) -> Table {
    return ExecutePlan(OptimizeQuery(BindQuery(sql)));
  }

  auto ExecuteCommand(const std::string &sql) -> std::string {
    auto table = ExecuteQuery(sql);
    if (table.GetRowCount() != 1 || table.GetColumnCount() != 1) {
      throw std::runtime_error("Expected a single-cell command result");
    }
    return table.GetRow(0)[0];
  }

 private:
  std::string db_name_;
  std::unique_ptr<OneBaseInstance> instance_;
};

}  // namespace onebase::eval