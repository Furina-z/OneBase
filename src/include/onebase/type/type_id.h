#pragma once

#include <cstdint>

namespace onebase {

enum class TypeId : uint8_t {
    INVALID = 0,
    BOOLEAN,
    INTEGER,
    FLOAT,
    VARCHAR,
};

}  // namespace onebase
