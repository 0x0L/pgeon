from distutils.core import setup
from Cython.Build import cythonize

import os
import numpy as np
import pyarrow as pa


ext_modules = cythonize("pgeon.pyx", compiler_directives={'language_level' : "3"})

for ext in ext_modules:
    # The Numpy C headers are currently required
    ext.include_dirs.append(np.get_include())
    ext.include_dirs.append(pa.get_include())
    ext.libraries.extend(pa.get_libraries())
    ext.library_dirs.extend(pa.get_library_dirs())

    #ext.extra_objects = ["/Users/xav/src/Pgeon/build/liblibpgeon.a"]
    ext.library_dirs.extend(["/Users/xav/src/Pgeon/python"])
    ext.libraries.extend(["libpgeon"])
    if os.name == 'posix':
        ext.extra_compile_args.append('-std=gnu++17')

    # ext.extra_link_args.append("-stdlib=libstdc++")

    # Try uncommenting the following line on Linux
    # if you get weird linker errors or runtime crashes
    # ext.define_macros.append(("_GLIBCXX_USE_CXX11_ABI", "0"))
    ext.define_macros.append(("HAS_UNCAUGHT_EXCEPTIONS", "0"))


setup(ext_modules=ext_modules)
