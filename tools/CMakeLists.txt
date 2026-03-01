# tools/CMakeLists.txt

add_executable(onebase_shell onebase_shell.cpp)
target_link_libraries(onebase_shell PRIVATE onebase_server_lib)

add_executable(b_plus_tree_printer b_plus_tree_printer.cpp)
target_link_libraries(b_plus_tree_printer PRIVATE onebase)
