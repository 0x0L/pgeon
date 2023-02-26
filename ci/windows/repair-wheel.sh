#! /usr/bin/env bash

wheel="$1"
dest_dir="$2"

# Install delvewheel in separate environment
pipx install delvewheel 
pipx environment
PIPX_HOME="$(pipx environment | grep -e 'PIPX_HOME=' | sed -re 's/PIPX_HOME=(.*)/\1/g')"
PIPX_HOME=$(cygpath "$PIPX_HOME")

"$PIPX_HOME/venvs/delvewheel/Scripts/python" ci/windows/repair-wheel.py "${wheel}" "${dest_dir}"
