# Macro needed by libpg_query
macro(disable_target_warnings TARGET_NAME)
  if(MSVC)
    target_compile_options(${TARGET_NAME} PRIVATE /W0)
  else()
    target_compile_options(${TARGET_NAME} PRIVATE -w)
  endif()
endmacro()

add_subdirectory(libpg_query)

# Use system packages (GoogleTest and fmt must be installed)
find_package(GTest REQUIRED)
find_package(fmt REQUIRED)
message(STATUS "Found GTest: ${GTest_VERSION}")
message(STATUS "Found fmt: ${fmt_VERSION}")
