add_library(onebase_storage
  disk/disk_manager.cpp
  page/page.cpp
  page/table_page.cpp
  page/b_plus_tree_internal_page.cpp
  page/b_plus_tree_leaf_page.cpp
  table/tuple.cpp
  table/table_heap.cpp
  index/b_plus_tree.cpp
  index/b_plus_tree_iterator.cpp
)
target_link_libraries(onebase_storage PUBLIC onebase_common onebase_type onebase_buffer)
