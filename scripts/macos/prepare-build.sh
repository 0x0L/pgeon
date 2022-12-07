#/usr/bin/env bash
set -e

brew install apache-arrow apache-arrow-glib libpq
python3 -m pip install pyarrow
