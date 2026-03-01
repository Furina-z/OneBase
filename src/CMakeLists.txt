# src/CMakeLists.txt — Master aggregator for all OneBase modules

add_subdirectory(common)
add_subdirectory(type)
add_subdirectory(buffer)
add_subdirectory(storage)
add_subdirectory(catalog)
add_subdirectory(execution)
add_subdirectory(concurrency)
add_subdirectory(binder)
add_subdirectory(optimizer)

# Aggregate all modules into a single interface library
add_library(onebase INTERFACE)
target_link_libraries(onebase INTERFACE
  onebase_binder
  onebase_optimizer
  onebase_execution
  onebase_catalog
  onebase_concurrency
  onebase_buffer
  onebase_storage
  onebase_buffer
  onebase_type
  onebase_common
)

# Server instance library (shared by server and tools)
add_library(onebase_server_lib STATIC onebase_server/onebase_instance.cpp)
target_link_libraries(onebase_server_lib PUBLIC onebase)
target_include_directories(onebase_server_lib PUBLIC ${PROJECT_SOURCE_DIR}/src/include)

# Server executable
find_package(Threads REQUIRED)
add_executable(onebase_server onebase_server/server_main.cpp)
target_link_libraries(onebase_server PRIVATE onebase_server_lib Threads::Threads)

# Client executable
add_executable(onebase_client
  onebase_client/client_main.cpp
)
target_link_libraries(onebase_client PRIVATE onebase_common)
