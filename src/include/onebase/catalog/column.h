#pragma once
#include <string>
#include "onebase/type/type_id.h"

namespace onebase {

class Column {
 public:
  Column(std::string name, TypeId type, uint32_t length = 0)
      : name_(std::move(name)), type_(type), length_(length) {
    if (type_ == TypeId::VARCHAR) {
      if (length_ == 0) { length_ = 256; }
      fixed_length_ = sizeof(uint32_t);  // offset into variable-length storage
      variable_length_ = length_;
    } else {
      fixed_length_ = GetFixedLengthFor(type_);
      variable_length_ = 0;
    }
  }

  auto GetName() const -> const std::string & { return name_; }
  auto GetType() const -> TypeId { return type_; }
  auto GetLength() const -> uint32_t { return length_; }
  auto GetFixedLength() const -> uint32_t { return fixed_length_; }
  auto GetVariableLength() const -> uint32_t { return variable_length_; }
  auto GetOffset() const -> uint32_t { return offset_; }
  void SetOffset(uint32_t offset) { offset_ = offset; }

  auto IsInlined() const -> bool { return type_ != TypeId::VARCHAR; }

  auto ToString() const -> std::string {
    return name_ + ":" + TypeIdToString(type_);
  }

 private:
  static auto GetFixedLengthFor(TypeId type) -> uint32_t {
    switch (type) {
      case TypeId::BOOLEAN: return 1;
      case TypeId::INTEGER: return 4;
      case TypeId::FLOAT: return 8;
      default: return 0;
    }
  }

  static auto TypeIdToString(TypeId type) -> std::string {
    switch (type) {
      case TypeId::BOOLEAN: return "BOOLEAN";
      case TypeId::INTEGER: return "INTEGER";
      case TypeId::FLOAT: return "FLOAT";
      case TypeId::VARCHAR: return "VARCHAR";
      default: return "INVALID";
    }
  }

  std::string name_;
  TypeId type_{TypeId::INVALID};
  uint32_t length_{0};
  uint32_t fixed_length_{0};
  uint32_t variable_length_{0};
  uint32_t offset_{0};
};

}  // namespace onebase
