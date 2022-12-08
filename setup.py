import os
import sys

import pyarrow as pa
from pybind11 import get_cmake_dir
from skbuild import setup
from skbuild.constants import skbuild_plat_name

py_arrow_dirs = ";".join(pa.get_library_dirs())

cmake_args = [
    f"-Dpybind11_DIR={get_cmake_dir()}",
    f"-DArrowPython_DIR={py_arrow_dirs}"
]

if sys.platform == "darwin":
    os.environ["MACOSX_DEPLOYMENT_TARGET"] = "10.13"

setup(
    cmake_args=cmake_args,
    packages=["pgeon"],
    package_dir={"pgeon": "src/python"},
)
