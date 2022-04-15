for f in sql/*.sql; do psql $POSTGRES_CONN -f $f; done
