from pybind11 import get_cmake_dir
from skbuild import setup

pybind11_cmake_dir = f"-Dpybind11_DIR={get_cmake_dir()}"

setup(cmake_args=[pybind11_cmake_dir])
