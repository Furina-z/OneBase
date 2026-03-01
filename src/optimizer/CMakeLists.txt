add_library(onebase_optimizer STATIC optimizer.cpp)
target_include_directories(onebase_optimizer PUBLIC ${PROJECT_SOURCE_DIR}/src/include)
target_link_libraries(onebase_optimizer PUBLIC onebase_execution)
target_compile_options(onebase_optimizer PRIVATE ${ONEBASE_CXX_FLAGS})
