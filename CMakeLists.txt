cmake_minimum_required(VERSION 3.10)
message(STATUS "Using CMake ${CMAKE_VERSION}")

project(pgeon VERSION 0.1)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(PostgreSQL REQUIRED)
message(STATUS "Using PostgreSQL ${PostgreSQL_VERSION_STRING}")

find_package(Arrow REQUIRED)
message(STATUS "Using Arrow ${ARROW_VERSION}")

# find_package(OpenMP REQUIRED)
# message(STATUS "Using OpenMP ${OpenMP_CXX_VERSION}")

add_library(pgeon SHARED)
target_sources(pgeon PRIVATE src/sql.cc src/decoders.cc)
target_link_libraries(pgeon PUBLIC arrow_shared PostgreSQL::PostgreSQL)
# target_link_libraries(pgeon PUBLIC OpenMP::OpenMP_CXX)

add_executable(pgeon-bin)
target_sources(pgeon-bin PRIVATE src/main.cc)
target_link_libraries(pgeon-bin PUBLIC pgeon)
set_target_properties(pgeon-bin PROPERTIES RUNTIME_OUTPUT_NAME pgeon)
