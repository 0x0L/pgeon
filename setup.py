import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
from Cython.Build import cythonize
from setuptools import Extension, setup


def get_pg_info():
    def run(x):
        return subprocess.check_output(["pg_config", x]).decode().strip()

    return {k: run("--" + k) for k in ("version", "includedir", "libdir")}


pa.create_library_symlinks()

include_dirs = ["include", "src", pa.get_include(), np.get_include()]
library_dirs = pa.get_library_dirs()
try:
    pg_config = get_pg_info()
    include_dirs.append(pg_config["includedir"])
    library_dirs.append(pg_config["libdir"])
except Exception:
    print("pg_config not found in PATH")

macros = [("_GLIBCXX_USE_CXX11_ABI", "0")]
if os.environ.get("CONDA_BUILD"):
    macros = None

cflags = {
    "win32": ["/std:c++latest"],
    "darwin": ["-std=c++17", "-mmacosx-version-min=10.14"],
    "linux": ["-std=c++17"],
}[sys.platform]

src_files = [str(p) for p in Path("src").glob("**/*.cc") if p.name != "cli.cc"]
extra_objects = [
    [
        "pgeon_cpp",
        {
            "sources": src_files,
            "include_dirs": include_dirs,
            "language": "c++",
            "macros": macros,
            "cflags": cflags,
        },
    ]
]

libraries = ["pgeon_cpp"] + pa.get_libraries()
if sys.platform == "win32":
    libraries.extend(["libpq", "Ws2_32"])
else:
    libraries.append("pq")

extensions = [
    Extension(
        name="pgeon._pgeon",
        sources=["python/_pgeon.pyx"],
        language="c++",
        define_macros=macros,
        include_dirs=include_dirs,
        libraries=libraries,
        library_dirs=library_dirs,
        extra_compile_args=cflags,
    )
]

setup(
    name="pgeon",
    ext_modules=cythonize(extensions),
    libraries=extra_objects,
    packages=["pgeon"],
    package_dir={"pgeon": "python"},
    include_package_data=False,
)
