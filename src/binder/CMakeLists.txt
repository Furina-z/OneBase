add_library(onebase_binder STATIC binder.cpp)
target_include_directories(onebase_binder PUBLIC ${PROJECT_SOURCE_DIR}/src/include)
target_link_libraries(onebase_binder PUBLIC onebase_catalog duckdb_pg_query)
target_compile_options(onebase_binder PRIVATE ${ONEBASE_CXX_FLAGS})
