if (SKBUILD)
    add_library(pgeon OBJECT)
    set_property(TARGET pgeon PROPERTY POSITION_INDEPENDENT_CODE ON)
else()
    add_library(pgeon SHARED)
endif()

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

target_include_directories(pgeon PUBLIC include)
target_include_directories(pgeon PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(pgeon PUBLIC arrow_shared PostgreSQL::PostgreSQL)

if(NOT SKBUILD)
    include(GNUInstallDirs)
    install(
        TARGETS pgeon
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
