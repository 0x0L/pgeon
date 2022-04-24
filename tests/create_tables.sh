#!/bin/bash
SCRIPT=$(readlink -f "${0}")
SCRIPTPATH=$(dirname "${SCRIPT}")

for f in $SCRIPTPATH/sql/*.sql; do
  echo Running ${f};
  psql -d $PGEON_TEST_DB -q -f ${f};
done
