add_library(onebase_common STATIC
    config.cpp
    rid.cpp
)
target_link_libraries(onebase_common PUBLIC fmt::fmt)
target_include_directories(onebase_common PUBLIC ${PROJECT_SOURCE_DIR}/src/include)
