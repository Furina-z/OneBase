#pragma once
#include <string>
#include <vector>
#include "onebase/catalog/column.h"

namespace onebase {

class Schema {
 public:
  explicit Schema(std::vector<Column> columns) : columns_(std::move(columns)) {
    uint32_t offset = 0;
    for (auto &col : columns_) {
      col.SetOffset(offset);
      if (col.IsInlined()) {
        offset += col.GetFixedLength();
      } else {
        offset += sizeof(uint32_t);  // pointer to variable-length data
      }
    }
    tuple_length_ = offset;
  }

  auto GetColumns() const -> const std::vector<Column> & { return columns_; }
  auto GetColumn(uint32_t idx) const -> const Column & { return columns_[idx]; }
  auto GetColumnCount() const -> uint32_t { return static_cast<uint32_t>(columns_.size()); }
  auto GetTupleLength() const -> uint32_t { return tuple_length_; }

  auto GetColumnIdx(const std::string &name) const -> uint32_t {
    for (uint32_t i = 0; i < columns_.size(); ++i) {
      if (columns_[i].GetName() == name) { return i; }
    }
    return UINT32_MAX;
  }

  auto ToString() const -> std::string {
    std::string result = "(";
    for (size_t i = 0; i < columns_.size(); ++i) {
      if (i > 0) { result += ", "; }
      result += columns_[i].ToString();
    }
    result += ")";
    return result;
  }

 private:
  std::vector<Column> columns_;
  uint32_t tuple_length_{0};
};

}  // namespace onebase
