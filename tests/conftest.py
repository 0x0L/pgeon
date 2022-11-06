import os
from pathlib import Path

import psycopg
import pytest

TESTS_FOLDER = Path(__file__).parent
SQL_FOLDER = TESTS_FOLDER / "sql"


def drop_all_tables(conninfo):

    with psycopg.connect(conninfo) as conn:
        conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED

        cur = conn.cursor()

        cur.execute(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name"
        )
        rows = cur.fetchall()
        for row in rows:
            cur.execute("drop table " + row[1] + " cascade")
        cur.close()
        conn.commit()


@pytest.fixture(scope="session")
def conninfo():
    yield os.environ.get(
        "PGEON_TEST_DB", "postgresql://postgres@localhost:5432/postgres"
    )


@pytest.fixture(autouse=True, scope="session")
def insert_data(conninfo):
    with psycopg.connect(conninfo) as conn:
        # conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
        for sql in SQL_FOLDER.glob("*.sql"):
            conn.execute(sql.read_text())

    yield
    drop_all_tables(conninfo)
