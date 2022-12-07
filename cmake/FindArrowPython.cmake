message(STATUS "Searching for ArrowPython...")

set(ArrowPython_CHECK_PATHS 
    "/usr/local/include"
    "/usr/local/homebrew/include" # Mac OS X
    "/opt/local/var/macports/software" # Mac OS X.
    "/opt/local/include"
    "/usr/include"
    "$ENV{CPLUS_INCLUDE_PATH}"
    "$ENV{CPATH}"
    "${CMAKE_SOURCE_DIR}/thirdparty/eigen"
    )

# First try to use the standard find_package that should be able to find the
# .cmake configuration files if installed in standard directories. 

# Saveup ArrowPython_DIR since the CONFIG overwrites it
set(TEMP_ArrowPython_DIR ${ArrowPython_DIR})
find_package(ArrowPython QUIET CONFIG)

if (NOT ArrowPython_FOUND)
    # Restore
    set(ArrowPython_DIR ${TEMP_ArrowPython_DIR})

    # Standard stuff did not work, so we try to locate ArrowPython in include paths
    find_path(ArrowPython_INCLUDE_DIR 
        NAMES
            arrow/api.h
        HINTS 
            ${ArrowPython_DIR}
            ${ArrowPython_DIR}/include
        PATH_SUFFIXES ArrowPython arrow
    )

    find_library(ArrowPython_LIBRARY
        NAMES
            arrow_python
            libarrow_python
        HINTS 
            ${ArrowPython_DIR}
            ${ArrowPython_DIR}/include
        PATH_SUFFIXES ArrowPython arrow
    )

endif (NOT ArrowPython_FOUND)


# Handle REQUIRED/QUIET optional arguments
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ArrowPython
    REQUIRED_VARS 
        ArrowPython_INCLUDE_DIR
        ArrowPython_LIBRARY
)

if(ArrowPython_FOUND AND NOT TARGET arrow_python_shared)
    add_library(arrow_python_shared SHARED IMPORTED)
    set_target_properties(arrow_python_shared PROPERTIES
        IMPORTED_LOCATION "${ArrowPython_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${ArrowPython_INCLUDE_DIR}"
    )
endif()