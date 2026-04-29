# test/CMakeLists.txt

include(GoogleTest)
cmake_policy(SET CMP0110 NEW)

# Convenience macro for adding tests
macro(onebase_add_test TEST_NAME TEST_SOURCE)
  add_executable(${TEST_NAME} ${TEST_SOURCE})
  target_link_libraries(${TEST_NAME} PRIVATE onebase GTest::gtest_main)
  add_test(NAME ${TEST_NAME} COMMAND $<TARGET_FILE:${TEST_NAME}>)
  set_tests_properties(${TEST_NAME} PROPERTIES
    ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0;LSAN_OPTIONS=verbosity=0:log_threads=0")
endmacro()

# Type tests
onebase_add_test(value_test type/value_test.cpp)

# Buffer pool tests (Lab 1)
onebase_add_test(lru_k_replacer_test buffer/lru_k_replacer_test.cpp)
onebase_add_test(buffer_pool_manager_test buffer/buffer_pool_manager_test.cpp)
onebase_add_test(page_guard_test buffer/page_guard_test.cpp)

# Storage tests
onebase_add_test(disk_manager_test storage/disk_manager_test.cpp)

# B+ tree tests (Lab 2)
onebase_add_test(b_plus_tree_insert_test storage/b_plus_tree_insert_test.cpp)
onebase_add_test(b_plus_tree_delete_test storage/b_plus_tree_delete_test.cpp)

# Execution tests (Lab 3)
onebase_add_test(executor_test execution/executor_test.cpp)
target_link_libraries(executor_test PRIVATE onebase_server_lib)
target_include_directories(executor_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Concurrency tests (Lab 4)
onebase_add_test(lock_manager_test concurrency/lock_manager_test.cpp)

# ──────────────────────────────────────────────
# Evaluation (graded) tests — custom main()
# ──────────────────────────────────────────────
macro(onebase_eval_test TEST_NAME TEST_SOURCE)
  add_executable(${TEST_NAME} ${TEST_SOURCE} eval/eval_main.cpp)
  target_link_libraries(${TEST_NAME} PRIVATE onebase onebase_server_lib GTest::gtest)
  target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  add_test(NAME ${TEST_NAME} COMMAND $<TARGET_FILE:${TEST_NAME}>)
  set_tests_properties(${TEST_NAME} PROPERTIES
    ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0;LSAN_OPTIONS=verbosity=0:log_threads=0")
endmacro()

onebase_eval_test(lab1_eval_test eval/lab1_eval_test.cpp)
onebase_eval_test(lab2_eval_test eval/lab2_eval_test.cpp)
onebase_eval_test(lab3_eval_test eval/lab3_eval_test.cpp)
onebase_eval_test(lab4_eval_test eval/lab4_eval_test.cpp)
