find_package(pybind11 REQUIRED)
find_package(ArrowPython REQUIRED)

pybind11_add_module(py_pgeon MODULE pgeon.cc)
set_target_properties(py_pgeon PROPERTIES OUTPUT_NAME "_pgeon")

target_link_libraries(py_pgeon PRIVATE pgeon arrow_python_shared)

install(
    TARGETS py_pgeon
    LIBRARY DESTINATION src/python
)
