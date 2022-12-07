message(STATUS "Searching for ArrowPython...")

# First try to use the standard find_package that should be able to find the
# .cmake configuration files if installed in standard directories. 

# Saveup ArrowPython_DIR since the CONFIG overwrites it
set(TEMP_ArrowPython_DIR ${ArrowPython_DIR})
find_package(ArrowPython QUIET CONFIG)

if (NOT ArrowPython_FOUND)
    message(WARNING "ArrowPython not found using config file...")
    message(STATUS "ArrowPython_DIR: ${ArrowPython_DIR}")
    # Restore
    set(ArrowPython_DIR ${TEMP_ArrowPython_DIR})

    # Standard stuff did not work, so we try to locate ArrowPython in include paths
    find_path(ArrowPython_INCLUDE_DIR 
        NAMES
            arrow/python/pyarrow.h
        HINTS 
            ${ArrowPython_DIR}
        PATH_SUFFIXES include
    )

    find_library(ArrowPython_LIBRARY
        NAMES
            arrow_python
            libarrow_python
        HINTS 
            ${ArrowPython_DIR}
        PATH_SUFFIXES include
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
    message(STATUS "ArrowPython lib: ${ArrowPython_LIBRARY}")
    add_library(arrow_python_shared SHARED IMPORTED)
    set_target_properties(arrow_python_shared PROPERTIES
        IMPORTED_LOCATION "${ArrowPython_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${ArrowPython_INCLUDE_DIR}"
    )
endif()