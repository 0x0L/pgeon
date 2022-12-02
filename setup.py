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

# For some reason skbuild seems to be confused about this
if sys.platform == "darwin":
    default_target = skbuild_plat_name().split("-")[1]
    target = os.getenv("MACOSX_DEPLOYMENT_TARGET", default_target)
    cmake_args.append(f"-DCMAKE_OSX_DEPLOYMENT_TARGET={target}")

setup(
    cmake_args=cmake_args,
    packages=["pgeon"],
    package_dir={"pgeon": "src/python"},
)
