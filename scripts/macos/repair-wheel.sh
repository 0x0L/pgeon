#!/usr/bin/env bash
set -e

echo "delocate wrapper script"

export DYLD_LIBRARY_PATH=$(python -c 'import pyarrow as pa; print(":".join(pa.get_library_dirs()))')

args=("$@")
dest_dir=${args[0]}
wheel=${args[1]}
delocate_arch = ${args[2]}

delocate-wheel --require-archs {delocate_archs} -w {dest_dir} -v {wheel}
rm -rf /project/_skbuild