import os

import pytest


@pytest.fixture(scope="session")
def dsn():
    yield "postgres://reader:NWDMCE5xdipIjRrp@hh-pgsql-public.ebi.ac.uk:5432/pfmegrnargs"
    # yield os.environ.get(
    #     "PGEON_TEST_DB", "postgresql://localhost:5432/postgres"
    # )
