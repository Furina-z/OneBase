#pragma once
#include <string>
#include <vector>
#include "onebase/common/rid.h"
#include "onebase/type/value.h"

namespace onebase {

class Schema;  // forward declaration

class Tuple {
 public:
  Tuple() = default;
  explicit Tuple(std::vector<Value> values);
  Tuple(const Tuple &other) = default;
  auto operator=(const Tuple &other) -> Tuple & = default;
  ~Tuple() = default;

  auto GetRID() const -> RID { return rid_; }
  void SetRID(const RID &rid) { rid_ = rid; }

  auto GetData() const -> const char * { return data_.data(); }
  auto GetLength() const -> uint32_t { return static_cast<uint32_t>(data_.size()); }

  auto GetValue(uint32_t column_idx) const -> Value;
  auto GetValue(const Schema *schema, uint32_t column_idx) const -> Value;
  auto GetValues() const -> const std::vector<Value> & { return values_; }

  auto ToString() const -> std::string;

  void SerializeTo(char *dest) const;
  void DeserializeFrom(const char *src, uint32_t size);

 private:
  RID rid_{};
  std::vector<char> data_;
  std::vector<Value> values_;
};

}  // namespace onebase
