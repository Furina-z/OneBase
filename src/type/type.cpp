#include "onebase/type/type.h"

#include "onebase/common/config.h"
#include "onebase/common/exception.h"
#include "onebase/common/macros.h"

namespace onebase {

auto Type::GetTypeSize(TypeId type) -> uint32_t {
    switch (type) {
        case TypeId::BOOLEAN:
            return sizeof(bool);
        case TypeId::INTEGER:
            return sizeof(int32_t);
        case TypeId::FLOAT:
            return sizeof(float);
        case TypeId::VARCHAR:
            return 0;  // Variable-length
        default:
            UNREACHABLE("Unknown type");
    }
}

auto Type::TypeIdToString(TypeId type) -> std::string {
    switch (type) {
        case TypeId::INVALID:
            return "INVALID";
        case TypeId::BOOLEAN:
            return "BOOLEAN";
        case TypeId::INTEGER:
            return "INTEGER";
        case TypeId::FLOAT:
            return "FLOAT";
        case TypeId::VARCHAR:
            return "VARCHAR";
        default:
            return "UNKNOWN";
    }
}

auto Type::IsCoercableTo(TypeId from, TypeId to) -> bool {
    if (from == to) {
        return true;
    }
    if (from == TypeId::INTEGER && to == TypeId::FLOAT) {
        return true;
    }
    return false;
}

}  // namespace onebase
