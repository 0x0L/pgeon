import os

import pytest


@pytest.fixture(scope="session")
def dsn():
    yield os.environ.get(
        "PGEON_TEST_DB", "postgresql://localhost:5432/postgres"
    )
