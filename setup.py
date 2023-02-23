import os
import subprocess
import sys
from pathlib import Path
from setuptools import setup, Extension

from Cython.Build import cythonize
import numpy as np
import pyarrow as pa

# Activate old ABI used by the pyarrow pypi wheels
macros = [("_GLIBCXX_USE_CXX11_ABI", "0")]
if os.environ.get("CONDA_BUILD"):
    macros = None

include_dir = "include"
source_dir = "src"
source_files = [str(p) for p in Path(source_dir).glob("**/*.cc")]

pg_include = []
pg_libdir = []
pg_lib = "pq"
try:
    pg_include = [subprocess.check_output(['pg_config', '--includedir']).decode().strip()]
    pg_libdir = [subprocess.check_output(['pg_config', '--libdir']).decode().strip()]
except:
    pass

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
            "sources": source_files,
            "include_dirs": [include_dir, source_dir, pa.get_include()] + pg_include,
            "language": "c++",
            "macros": macros,
            "cflags": extra_compile_args,
        },
    ]
]

extensions = [
    Extension(
        "_pgeon",
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
    package_dir={"": "python"},
    include_package_data=False,
)
