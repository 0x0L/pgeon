import sys
from pathlib import Path
from setuptools import setup, Extension

from Cython.Build import cythonize
import numpy as np
import pyarrow as pa

include_dir = "include"
source_dir = "src"
source_files = [str(p) for p in Path(source_dir).glob("**/*.cc")]

extra_compile_args = ["-std=c++17"]
if sys.platform == "darwin":
    extra_compile_args.append("-mmacosx-version-min=10.14")

pa.create_library_symlinks()

ext_libraries = [
    [
        "pgeon_cpp",
        {
            "sources": source_files,
            "include_dirs": [include_dir, source_dir, pa.get_include()],
            "language": "c++",
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
        libraries=["pgeon_cpp", "pq"] + pa.get_libraries(),
        library_dirs=pa.get_library_dirs(),
        extra_compile_args=extra_compile_args,
    )
]


setup(
    name="pgeon",
    ext_modules=cythonize(extensions),
    libraries=ext_libraries,
    package_dir={"": "python"},
    include_package_data=False
)
