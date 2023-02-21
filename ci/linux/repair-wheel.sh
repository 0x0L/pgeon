#/usr/bin/env bash
set -e

args=("$@")
dest_dir=${args[0]}
wheel=${args[1]}

auditwheel show {wheel}
auditwheel addtag -w {dest_dir} wheel