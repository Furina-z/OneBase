#include "onebase/storage/table/tuple.h"
#include <cstring>
#include <sstream>
#include "onebase/catalog/schema.h"

namespace onebase {

Tuple::Tuple(std::vector<Value> values) : values_(std::move(values)) {
  for (const auto &val : values_) {
    auto old_size = data_.size();
    auto val_size = val.GetSerializedSize();
    data_.resize(old_size + val_size);
    val.SerializeTo(data_.data() + old_size);
  }
}

auto Tuple::GetValue(uint32_t column_idx) const -> Value {
  if (column_idx < values_.size()) {
    return values_[column_idx];
  }
  return Value();
}

auto Tuple::GetValue(const Schema *schema, uint32_t column_idx) const -> Value {
  // If values are populated (tuple was created from vector of Values)
  if (!values_.empty() && column_idx < values_.size()) {
    return values_[column_idx];
  }
  // Otherwise, deserialize from raw data using schema
  const auto &column = schema->GetColumn(column_idx);
  return Value::DeserializeFrom(data_.data() + column.GetOffset(), column.GetType());
}

auto Tuple::ToString() const -> std::string {
  std::ostringstream os;
  os << "(";
  for (size_t i = 0; i < values_.size(); ++i) {
    if (i > 0) { os << ", "; }
    os << values_[i].ToString();
  }
  os << ")";
  return os.str();
}

void Tuple::SerializeTo(char *dest) const {
  std::memcpy(dest, data_.data(), data_.size());
}

void Tuple::DeserializeFrom(const char *src, uint32_t size) {
  data_.resize(size);
  std::memcpy(data_.data(), src, size);
}

}  // namespace onebase
