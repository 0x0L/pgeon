#/usr/bin/env bash
set -e

if command -v port &> /dev/null
then
    sudo port install postgresql11
else
    brew install postgresql
fi
