add_library(onebase_catalog
  catalog.cpp
)
target_link_libraries(onebase_catalog PUBLIC onebase_storage onebase_buffer onebase_type onebase_common)
