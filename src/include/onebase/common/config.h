#pragma once

#include <cstdint>

namespace onebase {

static constexpr int PAGE_SIZE = 4096;
static constexpr int DEFAULT_BUFFER_POOL_SIZE = 64;
static constexpr int DEFAULT_LRU_K = 10;
static constexpr int LOG_BUFFER_SIZE = PAGE_SIZE;
static constexpr int MAX_VARCHAR_SIZE = 256;

extern int BUFFER_POOL_SIZE;
extern int LRUK_REPLACER_K;

}  // namespace onebase

// Global alias used throughout the codebase
static constexpr int ONEBASE_PAGE_SIZE = onebase::PAGE_SIZE;
