import pytest
import pyarrow as pa

from pgeon import UserOptions, copy_query


def test_bad_dsn(dsn):
    with pytest.raises(IOError):
        copy_query("not_a_connection_string", "")

    with pytest.raises(IOError):
        copy_query(dsn + "_fizz", "")


def test_bad_query(dsn):
    with pytest.raises(IOError):
        copy_query(dsn, "smurfh")


def test_bad_options():
    with pytest.raises(pa.ArrowInvalid):
        UserOptions(default_numeric_precision=0).validate()
