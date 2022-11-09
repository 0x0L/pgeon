import os
import pyarrow as pa
import pyarrow.compute as pc

from pgeon import copy_query

db = os.environ["PGEON_TEST_DB"]
db = "postgresql://localhost/mytests"

# tbl = copy_query(db, query)
# tbl.equals(expects)

query = "SELECT * from numeric_table"

# Enumerated types
query = "SELECT * from person"

# Boolean type
query = "SELECT 'true'::boolean as t, 'false'::boolean as f"
expects = pa.table({"t": [True], "f": [False]})

# Numeric Types
# default is 22, 6 for numeric ?
query = """
SELECT
    x::smallint AS int16,
    x::integer AS int32,
    x::bigint AS int64,
    x::real AS float,
    x::double precision AS double,
    x::numeric(13, 3) AS numeric_13_3,
    x::numeric AS numeric_default
FROM generate_series(-3.5, 3.5, 1) AS x
"""
u = pa.array([-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5])
u_int = pc.round(u, round_mode="towards_infinity")
expects = pa.table(
    {
        "int16": u_int.cast(pa.int16()),
        "int32": u_int.cast(pa.int32()),
        "int64": u_int.cast(pa.int64()),
        "float": u.cast(pa.float32()),
        "double": u,
        "numeric_13_3": u.cast(pa.decimal128(13, 3)),
        "numeric_default": u.cast(pa.decimal128(22, 6)),
    }
)

# Monetary types
query = "SELECT '12.3342'::money"
expects = pa.table({"money": pa.array([12.33]).cast(pa.decimal128(22, 2))})

# Character types
query = "SELECT 'ok'::character(4)"
expects = pa.table({"bpchar": pa.array(["ok  "])})

query = "SELECT 'too long'::varchar(5)"
expects = pa.table({"varchar": pa.array(["too l"])})

query = "SELECT 'b'::char"  # TODO
expects = pa.table({"bpchar": pa.array(["b"])})

query = "SELECT 'b'::name"  # TODO
expects = pa.table({"bpchar": pa.array(["b"])})

query = r"SELECT '\xDEADBEEF'::text"
expects = pa.table({"text": pa.array([r"\xDEADBEEF"])})

# Binary data types
query = r"SELECT 'abc \153\154\155 \052\251\124'::bytea"
expects = pa.table({"bytea": pa.array([b"abc \153\154\155 \052\251\124"])})

# UUID type
query = "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid"
expects = pa.table(
    {
        "uuid": pa.array(
            [b"\xA0\xEE\xBC\x99\x9C\x0B\x4E\xF8\xBB\x6D\x6B\xB9\xBD\x38\x0A\x11"],
            pa.binary(16),
        )
    }
)

# Date/Time types
query = "SELECT TIMESTAMP '2001-01-01 14:00:00'"
expects = pa.table(
    {"timestamp": pa.array(["2001-01-01 14:00:00"]).cast(pa.timestamp("us"))}
)

query = "SELECT TIMESTAMP WITH TIME ZONE '2001-01-01 14:00:00+02:00'"
expects = pa.table(
    {
        "timestamptz": pa.array(["2001-01-01 14:00:00+02:00"]).cast(
            pa.timestamp("us", "utc")
        )
    }
)

query = "SELECT '1999-01-08'::date"
expects = pa.table(
    {"date": pa.array(["1999-01-08"]).cast(pa.timestamp("s")).cast(pa.date32())}
)

query = "SELECT TIME '14:00:00'"
expects = pa.table({"time": pa.array([50400000000]).cast(pa.time64("us"))})

query = "SELECT TIME WITH TIME ZONE '14:00:00+02:00'"
expects = pa.table({"timetz": pa.array([43200000000]).cast(pa.time64("us"))})

query = "SELECT '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval"
expects = pa.table({"interval": pa.array([pa.MonthDayNano([14, 3, 14706000000000])])})

# Geometric types
query = "SELECT '(1.2, 4.3)'::point"
expects = pa.table({"point": pa.array([{"x": 1.2, "y": 4.3}])})

query = "SELECT '{1.2, 4.3, 0.0}'::line"
expects = pa.table({"line": pa.array([{"A": 1.2, "B": 4.3, "C": 0.0}])})

query = "SELECT '((1.2, 4.3), (5.6, 7.8))'::lseg"
expects = pa.table(
    {
        "lseg": pa.array(
            [{"x1": 1.2, "y1": 4.3, "x2": 5.6, "y2": 7.8}],
            pa.struct(
                [(k, pa.float64()) for k in ("x1", "y1", "x2", "y2")]
            ),  # needed to maintain field order
        )
    }
)

# a bit strange
query = "SELECT '((1.2, 4.3), (5.6, 7.8))'::box"
expects = pa.table(
    {
        "box": pa.array(
            [{"x1": 5.6, "y1": 7.8, "x2": 1.2, "y2": 4.3}],
            pa.struct([(k, pa.float64()) for k in ("x1", "y1", "x2", "y2")]),
        )
    }
)

query = "SELECT '((1,2),(3,4))'::path"  # strangely that's closed
expects = pa.table(
    {
        "path": pa.array(
            [{"closed": True, "points": [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}]}]
        )
    }
)

query = "SELECT '[(1,2),(3,4)]'::path"  # open
expects = pa.table(
    {
        "path": pa.array(
            [{"closed": False, "points": [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}]}]
        )
    }
)

query = "SELECT '((1,2),(3,4))'::polygon"
expects = pa.table(
    {"polygon": pa.array([[{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}]])}
)

query = "SELECT '<(1.3, 3.4), 6>'::circle"
expects = pa.table(
    {
        "circle": pa.array(
            [{"x": 1.3, "y": 3.4, "r": 6}],
            pa.struct([(k, pa.float64()) for k in ("x", "y", "r")]),
        )
    }
)

# Network address types
query = "SELECT '::ffff:1.2.3.0/120'::cidr"
expects = pa.table(
    {
        "cidr": pa.array(
            [
                {
                    "family": 3,
                    "bits": 120,
                    "is_cidr": True,
                    "ipaddr": b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x01\x02\x03\x00",
                }
            ],
            pa.struct(
                [
                    ("family", pa.uint8()),
                    ("bits", pa.uint8()),
                    ("is_cidr", pa.bool_()),
                    ("ipaddr", pa.binary()),
                ]
            ),
        )
    }
)

query = "SELECT '192.168.100.128/25'::inet"
expects = pa.table(
    {
        "inet": pa.array(
            [
                {
                    "family": 2,
                    "bits": 25,
                    "is_cidr": False,
                    "ipaddr": b"\xC0\xA8\x64\x80",
                }
            ],
            pa.struct(
                [
                    ("family", pa.uint8()),
                    ("bits", pa.uint8()),
                    ("is_cidr", pa.bool_()),
                    ("ipaddr", pa.binary()),
                ]
            ),
        )
    }
)

query = "SELECT '08:00:2b:01:02:03'::macaddr"
expects = pa.table({"macaddr": pa.array([b"\x08\x00\x2B\x01\x02\x03"], pa.binary(6))})

query = "SELECT '08:00:2b:01:02:03:04:05'::macaddr8"
expects = pa.table(
    {"macaddr8": pa.array([b"\x08\x00\x2B\x01\x02\x03\x04\x05"], pa.binary(8))}
)

# Bit string types
# TODO which interface should it be ?
query = "SELECT B'101'::bit(3)"
query = "SELECT B'10'::bit varying(5)"

# Text search types
# TODO check
query = "SELECT 'a fat cat sat on a mat and ate a fat rat'::tsvector"
query = "SELECT $$the lexeme '    ' contains spaces$$::tsvector"
query = "SELECT $$the lexeme 'Joe''s' contains a quote$$::tsvector"
query = "SELECT 'a:1 fat:2 cat:3 sat:4 on:5 a:6 mat:7 and:8 ate:9 a:10 fat:11 rat:12'::tsvector"
query = "SELECT 'a:1A fat:2B,4C cat:5D'::tsvector"  # not supported
query = "SELECT 'The Fat Rats'::tsvector"
query = "SELECT to_tsvector('english', 'The Fat Rats')"
query = "SELECT 'fat & rat'::tsquery"
query = "SELECT 'fat & (rat | cat)'::tsquery"
query = "SELECT 'fat & rat & ! cat'::tsquery"
query = "SELECT 'fat:ab & cat'::tsquery"
query = "SELECT 'super:*'::tsquery"
query = "SELECT to_tsquery('Fat:ab & Cats')"
query = "SELECT to_tsvector( 'postgraduate' ) @@ to_tsquery( 'postgres:*' )"
query = "SELECT to_tsvector( 'postgraduate' ), to_tsquery( 'postgres:*' )"

# XML type
query = """SELECT * FROM XMLPARSE (DOCUMENT '<?xml version="1.0"?><book><title>Manual</title><chapter>...</chapter></book>')"""
query = "SELECT * FROM XMLPARSE (CONTENT 'abc<foo>bar</foo><bar>foo</bar>')"

# JSON types
query = "SELECT * from json_table"
query = "SELECT (a_jsonb->>'a')::int from json_table"
query = "SELECT '$.key'::jsonpath"

# Arrays
query = "SELECT '{1000, 2000}'::bigint[]"
expects = pa.table({"int8": pa.array([[1000, 2000]])})

query = "SELECT * from sal_emp"

# Composite types
# arrays are flattened
query = "SELECT * from on_hand"

# TODO Range types
query = "SELECT int4range(10, 20)"
query = "SELECT '{[3,7), [8,9)}'::int4multirange"
query = "SELECT int8range(10, 20)"
query = "SELECT numrange(11.1, 22.2)"
query = "SELECT nummultirange(numrange(1.0, 14.0), numrange(20.0, 25.0))"
# tsrange, tstzrange, daterange

# TODO domain types

# Object identifier types
query = "SELECT * FROM pg_attribute WHERE attrelid = 'on_hand'::regclass"

# TODO `pg_lsn` type
query = "SELECT pg_current_snapshot()"

# pseudo types
query = "SELECT 'NULL'::void"

# Hstore extension
query = "SELECT 'a=>1,b=>2'::hstore"
query = "SELECT 'a=>1,a=>2'::hstore"
query = """SELECT 'a=>1,b=>""'::hstore"""
query = """SELECT 'a=>1,b=>NULL'::hstore"""

# does not work, apparently no schema
query = "SELECT ROW('1'::real, '2'::real)"

# dictionary encoded control ?
# fixed size handling => templates ?
