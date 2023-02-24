add_library(pgeon SHARED)

target_sources(pgeon PRIVATE
    builder/datetime.cc
    builder/geometric.cc
    builder/misc.cc
    builder/nested.cc
    builder/network.cc
    builder/numeric.cc
    builder/stringlike.cc
    builder/text_search.cc
    builder.cc
    table_builder.cc
    pg_interface.cc
    api.cc
)

target_include_directories(pgeon PUBLIC ../include)
target_include_directories(pgeon PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(pgeon PRIVATE arrow_shared PostgreSQL::PostgreSQL)

include(GNUInstallDirs)
install(
    TARGETS pgeon
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

add_executable(pgeon-cli cli.cc)
target_link_libraries(pgeon-cli PRIVATE pgeon arrow_shared PostgreSQL::PostgreSQL)
