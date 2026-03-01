#pragma once

#include <cassert>
#include <stdexcept>

namespace onebase {

#define DISALLOW_COPY(ClassName)                                    \
    ClassName(const ClassName &) = delete;                         \
    auto operator=(const ClassName &)->ClassName & = delete;

#define DISALLOW_MOVE(ClassName)                                    \
    ClassName(ClassName &&) = delete;                               \
    auto operator=(ClassName &&)->ClassName & = delete;

#define DISALLOW_COPY_AND_MOVE(ClassName) \
    DISALLOW_COPY(ClassName)             \
    DISALLOW_MOVE(ClassName)

#define ONEBASE_ASSERT(expr, message)               \
    if (!(expr)) {                                  \
        throw std::logic_error(message);            \
    }

#define UNREACHABLE(message) throw std::logic_error(message)

}  // namespace onebase
