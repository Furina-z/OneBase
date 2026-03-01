#pragma once

#include <fmt/format.h>

namespace onebase {

#define LOG_INFO(...)  fmt::print("[INFO]  {}\n", fmt::format(__VA_ARGS__))
#define LOG_WARN(...)  fmt::print("[WARN]  {}\n", fmt::format(__VA_ARGS__))
#define LOG_ERROR(...) fmt::print("[ERROR] {}\n", fmt::format(__VA_ARGS__))

#ifdef NDEBUG
#define LOG_DEBUG(...) ((void)0)
#else
#define LOG_DEBUG(...) fmt::print("[DEBUG] {}\n", fmt::format(__VA_ARGS__))
#endif

}  // namespace onebase
