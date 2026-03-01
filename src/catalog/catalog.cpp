#include "onebase/catalog/catalog.h"
#include <stdexcept>

namespace onebase {

auto Catalog::CreateTable(const std::string &table_name, const Schema &schema) -> TableInfo * {
  if (table_names_.count(table_name) > 0) {
    return nullptr;  // Table already exists
  }
  auto table_oid = next_table_oid_++;
  auto table_heap = std::make_unique<TableHeap>(bpm_);
  auto table_info = std::make_unique<TableInfo>(schema, table_name, std::move(table_heap), table_oid);
  auto *raw = table_info.get();
  tables_[table_oid] = std::move(table_info);
  table_names_[table_name] = table_oid;
  return raw;
}

auto Catalog::GetTable(const std::string &table_name) const -> TableInfo * {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) { return nullptr; }
  return tables_.at(it->second).get();
}

auto Catalog::GetTable(table_oid_t oid) const -> TableInfo * {
  auto it = tables_.find(oid);
  if (it == tables_.end()) { return nullptr; }
  return it->second.get();
}

auto Catalog::GetAllTables() const -> std::vector<TableInfo *> {
  std::vector<TableInfo *> result;
  result.reserve(tables_.size());
  for (const auto &[oid, info] : tables_) {
    result.push_back(info.get());
  }
  return result;
}

auto Catalog::CreateIndex(const std::string &index_name, const std::string &table_name,
                          const std::vector<uint32_t> &key_attrs) -> IndexInfo * {
  auto *table_info = GetTable(table_name);
  if (table_info == nullptr) { return nullptr; }

  // Build key schema from key attributes
  std::vector<Column> key_columns;
  for (auto attr : key_attrs) {
    key_columns.push_back(table_info->schema_.GetColumn(attr));
  }
  Schema key_schema(key_columns);

  auto index_oid = next_index_oid_++;
  auto index_info = std::make_unique<IndexInfo>(
      std::move(key_schema), index_name, table_name, index_oid, key_attrs);
  auto *raw = index_info.get();
  indexes_[index_oid] = std::move(index_info);
  index_names_[table_name][index_name] = index_oid;
  return raw;
}

auto Catalog::GetIndex(const std::string &index_name, const std::string &table_name) const -> IndexInfo * {
  auto table_it = index_names_.find(table_name);
  if (table_it == index_names_.end()) { return nullptr; }
  auto idx_it = table_it->second.find(index_name);
  if (idx_it == table_it->second.end()) { return nullptr; }
  return indexes_.at(idx_it->second).get();
}

auto Catalog::GetTableIndexes(const std::string &table_name) const -> std::vector<IndexInfo *> {
  std::vector<IndexInfo *> result;
  auto table_it = index_names_.find(table_name);
  if (table_it == index_names_.end()) { return result; }
  for (const auto &[name, oid] : table_it->second) {
    result.push_back(indexes_.at(oid).get());
  }
  return result;
}

}  // namespace onebase
