#pragma once
#include <string>
#include "onebase/common/types.h"

namespace onebase {

class Index {
 public:
  explicit Index(std::string name) : name_(std::move(name)) {}
  virtual ~Index() = default;
  auto GetName() const -> const std::string & { return name_; }

 private:
  std::string name_;
};

}  // namespace onebase
