#!/usr/bin/env bash
set -e

echo "auditwheel wrapper script"

export LD_LIBRARY_PATH=$(python -c 'import pyarrow as pa; print(":".join(pa.get_library_dirs()))')

args=("$@")
dest_dir=${args[0]}
wheel=${args[1]}

auditwheel repair -w ${dest_dir} ${wheel}
rm -rf /project/_skbuild