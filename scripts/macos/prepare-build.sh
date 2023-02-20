#/usr/bin/env bash
set -e

if command -v port &> /dev/null
then 
    sudo port install apache-arrow postgresql11
else
    brew install apache-arrow apache-arrow-glib libpq
fi

python3 -m pip install pyarrow
