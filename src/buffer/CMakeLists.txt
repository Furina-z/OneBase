add_library(onebase_buffer
  lru_k_replacer.cpp
  buffer_pool_manager.cpp
  page_guard.cpp
)
target_link_libraries(onebase_buffer PUBLIC onebase_common)
