import pyarrow as pa

from pgeon import UserOptions, copy_query


def test_string_as_dictionaries(dsn):
    query = "SELECT 'a' AS c"
    options = UserOptions(string_as_dictionaries=True)
    expected = pa.table({"c": pa.array(["a"]).dictionary_encode()})

    tbl = copy_query(dsn, query, options)
    assert tbl.equals(expected)
