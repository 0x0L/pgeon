cmake_minimum_required(VERSION 3.10)
message(STATUS "Using CMake ${CMAKE_VERSION}")

project(pgeon VERSION 0.1)

find_package(PostgreSQL REQUIRED)
message(STATUS "Using PostgreSQL ${PostgreSQL_VERSION_STRING}")

# -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl@3
find_package(Arrow REQUIRED)
message(STATUS "Using Arrow ${ARROW_VERSION}")

find_package(Parquet REQUIRED
    PATHS ${Arrow_DIR}
    NO_DEFAULT_PATH
)
message(STATUS "Using Parquet ${PARQUET_VERSION}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

file(GLOB CppSources src/*.cc)
add_executable(pgeon ${CppSources})
target_link_libraries(pgeon PRIVATE arrow_shared parquet_shared PostgreSQL::PostgreSQL)
