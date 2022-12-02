#/usr/bin/env bash
set -e

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
bash "${SCRIPTPATH}/deps/arrow-deps.sh"
bash "${SCRIPTPATH}/deps/pg-deps.sh"