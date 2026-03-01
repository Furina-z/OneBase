add_library(onebase_concurrency
  lock_manager.cpp
  transaction_manager.cpp
)
target_link_libraries(onebase_concurrency PUBLIC onebase_common)
