cmake_minimum_required(VERSION 3.12)
message(STATUS "Using CMake ${CMAKE_VERSION}")

# Main project configuration
project(
    pgeon
    VERSION 0.1
    DESCRIPTION "Apache Arrow PostgreSQL copy connector"
    HOMEPAGE_URL "https://github.com/0x0L/pgeon"
    LANGUAGES CXX
)

# Build options
option(BUILD_PYTHON_BINDINGS "Build Python bindings using cython" ON)

# Compiler settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(PostgreSQL REQUIRED)
message(STATUS "Using PostgreSQL ${PostgreSQL_VERSION_STRING}")

find_package(Arrow REQUIRED)
message(STATUS "Using Arrow ${ARROW_VERSION}")

# find_package(OpenMP REQUIRED)
# message(STATUS "Using OpenMP ${OpenMP_CXX_VERSION}")

# Main library
add_library(pgeon SHARED)

target_sources(pgeon PRIVATE
    src/pgeon/builder/datetime.cc
    src/pgeon/builder/geometric.cc
    src/pgeon/builder/nested.cc
    src/pgeon/builder/network.cc
    src/pgeon/builder/numeric.cc
    src/pgeon/builder/stringlike.cc
    src/pgeon/builder.cc
    src/pgeon/table_builder.cc
    src/pgeon/sql_interface.cc
    src/pgeon/api.cc
)

target_include_directories(pgeon PRIVATE src)

target_include_directories(pgeon PUBLIC include)

target_link_libraries(pgeon PUBLIC arrow_shared PostgreSQL::PostgreSQL)
# target_link_libraries(pgeon PUBLIC OpenMP::OpenMP_CXX)

# Python bindings
if(BUILD_PYTHON_BINDINGS)
  find_package(PythonInterp 3.10 REQUIRED)
  find_package(PythonLibs 3.10 REQUIRED)

  # Prepare Python's setup.cfg
  file(GENERATE
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/python/setup.cfg
    CONTENT
"[build_ext]
library_dirs=$<TARGET_FILE_DIR:pgeon>
include_dirs=$<JOIN:$<TARGET_PROPERTY:pgeon,INCLUDE_DIRECTORIES>,:>
define=$<IF:$<BOOL:${CODE_COVERAGE}>,CYTHON_TRACE,>
[build_sphinx]
build_dir=${CMAKE_CURRENT_BINARY_DIR}/docs/python
")

  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/python.stamp
    COMMAND ${PYTHON_EXECUTABLE} setup.py build_ext --force
    COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/python.stamp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/python
    DEPENDS
      ${CMAKE_CURRENT_SOURCE_DIR}/python/setup.py
      ${CMAKE_CURRENT_SOURCE_DIR}/python/setup.cfg
      ${CMAKE_CURRENT_SOURCE_DIR}/python/pgeon.pyx
      pgeon
    COMMENT "Building Python bindings to 'pgeon' library"
  )
  add_custom_target(pgeon-python ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/python.stamp)
endif()
