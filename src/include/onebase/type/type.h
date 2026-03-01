#pragma once

#include "onebase/type/type_id.h"
#include "onebase/type/value.h"

#include <string>

namespace onebase {

class Type {
   public:
    static auto GetTypeSize(TypeId type) -> uint32_t;
    static auto TypeIdToString(TypeId type) -> std::string;
    static auto IsCoercableTo(TypeId from, TypeId to) -> bool;
};

}  // namespace onebase
