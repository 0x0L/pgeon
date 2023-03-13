add_library(pgeon SHARED)

target_sources(pgeon PRIVATE
    pgeon/builder/datetime.cc
    pgeon/builder/geometric.cc
    pgeon/builder/misc.cc
    pgeon/builder/nested.cc
    pgeon/builder/network.cc
    pgeon/builder/numeric.cc
    pgeon/builder/stringlike.cc
    pgeon/builder/text_search.cc
    pgeon/builder.cc
    pgeon/table_builder.cc
    pgeon/pg_interface.cc
    pgeon/api.cc
)

target_include_directories(pgeon PUBLIC ../include)
target_include_directories(pgeon PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(pgeon PRIVATE arrow_shared PostgreSQL::PostgreSQL)

include(GNUInstallDirs)
install(
    TARGETS pgeon
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

find_package(Parquet REQUIRED)
add_executable(pgeon-cli cli.cc)
target_link_libraries(pgeon-cli PRIVATE pgeon arrow_shared parquet_shared PostgreSQL::PostgreSQL)
