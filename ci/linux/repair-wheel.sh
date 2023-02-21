#/usr/bin/env bash
set -e

args=("$@")
wheel=${args[0]}
dest_dir=${args[1]}

auditwheel show ${wheel}
auditwheel repair --exclude libarrow.so.1100 --exclude libarrow_python.so -w ${dest_dir} ${wheel}