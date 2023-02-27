#/usr/bin/env bash
set -e

if [ "$RUNNER_OS" == "Linux" ]; then
  echo "Install postgres"
  yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
  yum install -y postgresql15-server postgresql15-devel

  ln -sf /usr/pgsql-15/bin/pg_config /usr/bin/

  export PATH=/usr/pgsql-15/bin:$PATH
  export RUNNER_TEMP="/tmp"
fi

echo "Set up database"

PGDATA="$RUNNER_TEMP/pgdata"

if [ "$RUNNER_OS" == "Linux" ]; then
  su postgres -c "initdb --locale=C -E UTF-8 $PGDATA"
  su postgres -c "pg_ctl -D $PGDATA start"
else
  initdb --locale=C -E UTF-8 $PGDATA
  pg_ctl -D $PGDATA start
fi
