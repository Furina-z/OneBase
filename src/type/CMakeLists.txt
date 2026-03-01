add_library(onebase_type STATIC
    value.cpp
    type.cpp
)
target_link_libraries(onebase_type PUBLIC onebase_common)
target_include_directories(onebase_type PUBLIC ${PROJECT_SOURCE_DIR}/src/include)
