#/usr/bin/env bash
set -e

yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm || true
yum install -y postgresql11-devel || true

ln -sf /usr/pgsql-11/bin/pg_config /usr/bin/