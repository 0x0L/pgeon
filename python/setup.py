import os
from distutils.core import setup

import numpy as np
import pyarrow as pa
from Cython.Build import cythonize

ext_modules = cythonize(
    "pgeon.pyx",
    compiler_directives={"language_level": "3"},
)

for ext in ext_modules:
    # The Numpy C headers are currently required
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.libraries.extend(pa.get_libraries())
    ext.library_dirs.extend(pa.get_library_dirs())

    ext.extra_compile_args.append("-Wno-unused-variable")
    ext.libraries.append("pgeon")

    if os.name == "posix":
        ext.extra_compile_args.append("-std=gnu++17")

setup(name="pgeon", ext_modules=ext_modules)
