#!/usr/bin/env bash
set -e

echo "delocate wrapper script"

pyarrow_dir=$(python -c 'import pyarrow as pa; print(pa.get_library_dirs()[0])')
pyarrow_version=$(python -c "import pyarrow as pa; print(pa.__version__.replace('.', ''))")

echo "${pyarrow_version}"

# Add a symlink to allow delocate-wheel to find libarrow_python
DYLIB_HACK_DIR="$PWD/_skbuild/pyarrow_hack"
mkdir $DYLIB_HACK_DIR
ln -s "${pyarrow_dir}/libarrow_python.dylib" $DYLIB_HACK_DIR
# ln -s "${pyarrow_dir}/libarrow.${pyarrow_version}.dylib" $DYLIB_HACK_DIR

# Debug
ls -al $DYLIB_HACK_DIR
ls -al ${pyarrow_dir}

export DYLD_LIBRARY_PATH=$DYLIB_HACK_DIR

args=("$@")
dest_dir=${args[0]}
wheel=${args[1]}
delocate_arch=${args[2]}

echo "dest_dir: ${dest_dir}"
echo "wheel: ${wheel}"
echo "delocate_arch: ${delocate_arch}"

delocate-wheel --require-archs ${delocate_arch} -w ${dest_dir} -v ${wheel}
rm -rf "$PWD/_skbuild"
