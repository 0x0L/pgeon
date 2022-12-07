cmake_minimum_required(VERSION 3.14)
message(STATUS "Using CMake ${CMAKE_VERSION}")

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")


# Main project configuration
project(
    pgeon
    VERSION 0.1
    DESCRIPTION "Apache Arrow PostgreSQL connector"
    HOMEPAGE_URL "https://github.com/0x0L/pgeon"
    LANGUAGES CXX
)

cmake_policy(SET CMP0054 NEW)

# Compiler settings
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(PostgreSQL REQUIRED)
message(STATUS "Using PostgreSQL ${PostgreSQL_VERSION_STRING} ${PostgreSQL_LIBRARIES}")

find_package(Arrow REQUIRED)
message(STATUS "Using Arrow ${ARROW_FULL_SO_VERSION}")

add_subdirectory(src/cpp)

# To build python wrapper, use pip or build
if (SKBUILD)
  add_subdirectory(src/python)
endif()
