#/usr/bin/env bash
set -e

if [ "$RUNNER_OS" == "Linux" ]; then
echo "Install postgres"
yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm || true
yum install -y postgresql11-devel || true

ln -sf /usr/pgsql-11/bin/pg_config /usr/bin/
export RUNNER_TEMP="/tmp"
fi

echo "Set up database"

export PGDATA="$RUNNER_TEMP/pgdata"
export PWFILE="$RUNNER_TEMP/pwfile"

echo 'testpwd' > $PWFILE

initdb \
  --username="testuser" \
  --pwfile="$PWFILE" \
  --auth="scram-sha-256" \
  --encoding="UTF-8" \
  --locale="en_US.UTF-8" \
  --no-instructions

echo "unix_socket_directories = ''" >> "$PGDATA/postgresql.conf"
echo "port = 1234" >> "$PGDATA/postgresql.conf"
pg_ctl start

cat <<EOF > "$PGDATA/pg_service.conf"
[testuser]
host=localhost
port=1234
user=testuser
password=testpwd
dbname=testdb
EOF

createdb -O testuser testdb
