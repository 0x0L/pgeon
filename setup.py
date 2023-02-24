import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
from Cython.Build import cythonize
from setuptools import Extension, setup


def pg_config_dir(arg):
    return subprocess.check_output(["pg_config", arg]).decode().strip()


# Activate old ABI used by the pyarrow pypi wheels
macros = [("_GLIBCXX_USE_CXX11_ABI", "0")]
if os.environ.get("CONDA_BUILD"):
    macros = None

include_dir = "include"
src_dir = "src"
src_files = [str(p) for p in Path(src_dir).glob("**/*.cc") if p.name != "cli.cc"]

pg_lib = "pq"
pg_include = []
pg_libdir = []
try:
<<<<<<< HEAD
    pg_include = [pg_config_dir("--includedir")]
    pg_libdir = [pg_config_dir("--libdir")]
except Exception:
    print("pg_config not found in PATH")
=======
    pg_include = [subprocess.check_output(['pg_config', '--includedir']).decode().strip()]
    pg_libdir = [subprocess.check_output(['pg_config', '--libdir']).decode().strip()]
except:
    raise(RuntimeError("pg_config needs to be available in the PATH"))
>>>>>>> 8d67092 (adding postgres to path on win)

extra_compile_args = ["-std=c++17"]
if sys.platform == "darwin":
    extra_compile_args.append("-mmacosx-version-min=10.14")
elif sys.platform == "win32":
    pg_lib = "libpq"
    extra_compile_args = ["/std:c++latest"]

pa.create_library_symlinks()

ext_libraries = [
    [
        "pgeon_cpp",
        {
            "sources": src_files,
            "include_dirs": [include_dir, src_dir, pa.get_include()] + pg_include,
            "language": "c++",
            "macros": macros,
            "cflags": extra_compile_args,
        },
    ]
]

extensions = [
    Extension(
        name="pgeon._pgeon",
        sources=["python/_pgeon.pyx"],
        language="c++",
        include_dirs=[include_dir, np.get_include(), pa.get_include()],
        libraries=["pgeon_cpp", pg_lib] + pa.get_libraries(),
        library_dirs=pg_libdir + pa.get_library_dirs(),
        extra_compile_args=extra_compile_args,
    )
]


setup(
    name="pgeon",
    ext_modules=cythonize(extensions),
    libraries=ext_libraries,
    packages=["pgeon"],
    package_dir={"pgeon": "python"},
    include_package_data=False,
)
