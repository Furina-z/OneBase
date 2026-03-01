add_library(onebase_execution
  executors/seq_scan_executor.cpp
  executors/index_scan_executor.cpp
  executors/insert_executor.cpp
  executors/delete_executor.cpp
  executors/update_executor.cpp
  executors/nested_loop_join_executor.cpp
  executors/hash_join_executor.cpp
  executors/aggregation_executor.cpp
  executors/sort_executor.cpp
  executors/limit_executor.cpp
  executors/projection_executor.cpp
  executors/executor_factory.cpp
  executors/execution_engine.cpp
)
target_link_libraries(onebase_execution PUBLIC onebase_catalog onebase_storage onebase_buffer onebase_type onebase_common)
