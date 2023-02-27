import sys

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from pgeon import copy_query

u = pa.array([-3.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.5])
u_int = pc.round(u, round_mode="towards_infinity")

tests = [
    (
        "SELECT * FROM (VALUES (True), (False)) AS foo",
        pa.table({"column1": [True, False]}),
    ),
    (
        """SELECT
x::smallint AS int16,
x::integer AS int32,
x::bigint AS int64,
x::real AS float,
x::double precision AS double,
x::numeric(13, 3) AS numeric_13_3,
x::numeric AS numeric_default
FROM generate_series(-3.5, 3.5, 1) AS x
""",
        pa.table(
            {
                "int16": u_int.cast(pa.int16()),
                "int32": u_int.cast(pa.int32()),
                "int64": u_int.cast(pa.int64()),
                "float": u.cast(pa.float32()),
                "double": u,
                "numeric_13_3": u.cast(pa.decimal128(13, 3)),
                "numeric_default": u.cast(pa.decimal128(22, 6)),
            }
        ),
    ),
    (
        "SELECT '12.3342'::money",
        pa.table({"money": pa.array([12.33]).cast(pa.decimal128(22, 2))}),
    ),
    ("SELECT 'ok'::character(4)", pa.table({"bpchar": pa.array(["ok  "])})),
    ("SELECT 'too long'::varchar(5)", pa.table({"varchar": pa.array(["too l"])})),
    ("SELECT 'b'::char", pa.table({"bpchar": pa.array(["b"])})),
    ("SELECT 'b'::name", pa.table({"name": pa.array(["b"])})),
    (r"SELECT '\xDEADBEEF'::text", pa.table({"text": pa.array([r"\xDEADBEEF"])})),
    (
        r"SELECT 'abc \153\154\155 \052\251\124'::bytea",
        pa.table({"bytea": pa.array([b"abc \153\154\155 \052\251\124"])}),
    ),
    (
        "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
        pa.table(
            {
                "uuid": pa.array(
                    [
                        b"\xA0\xEE\xBC\x99\x9C\x0B\x4E\xF8\xBB\x6D\x6B\xB9\xBD\x38\x0A\x11"
                    ],
                    pa.binary(16),
                )
            }
        ),
    ),
    (
        "SELECT TIMESTAMP '2001-01-01 14:00:00'",
        pa.table(
            {"timestamp": pa.array(["2001-01-01 14:00:00"]).cast(pa.timestamp("us"))}
        ),
    ),
    (
        "SELECT TIMESTAMP WITH TIME ZONE '2001-01-01 14:00:00+02:00'",
        pa.table(
            {
                "timestamptz": pa.array(["2001-01-01 14:00:00+02:00"]).cast(
                    pa.timestamp("us", "utc")
                )
            }
        ),
    ),
    (
        "SELECT '1999-01-08'::date",
        pa.table(
            {"date": pa.array(["1999-01-08"]).cast(pa.timestamp("s")).cast(pa.date32())}
        ),
    ),
    (
        "SELECT TIME '14:00:00'",
        pa.table({"time": pa.array([50400000000]).cast(pa.time64("us"))}),
    ),
    (
        "SELECT TIME WITH TIME ZONE '14:00:00+02:00'",
        pa.table({"timetz": pa.array([43200000000]).cast(pa.time64("us"))}),
    ),
    (
        "SELECT '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval",
        pa.table({"interval": pa.array([pa.MonthDayNano([14, 3, 14706000000000])])}),
    ),
    (
        "SELECT '(1.2, 4.3)'::point",
        pa.table({"point": pa.array([{"x": 1.2, "y": 4.3}])}),
    ),
    (
        "SELECT '{1.2, 4.3, 0.0}'::line",
        pa.table({"line": pa.array([{"A": 1.2, "B": 4.3, "C": 0.0}])}),
    ),
    (
        "SELECT '((1.2, 4.3), (5.6, 7.8))'::lseg",
        pa.table(
            {
                "lseg": pa.array(
                    [{"x1": 1.2, "y1": 4.3, "x2": 5.6, "y2": 7.8}],
                    pa.struct(
                        [(k, pa.float64()) for k in ("x1", "y1", "x2", "y2")]
                    ),  # needed to maintain field order
                )
            }
        ),
    ),
    (
        "SELECT '((1.2, 4.3), (5.6, 7.8))'::box",
        pa.table(
            {
                "box": pa.array(
                    [{"x1": 5.6, "y1": 7.8, "x2": 1.2, "y2": 4.3}],
                    pa.struct([(k, pa.float64()) for k in ("x1", "y1", "x2", "y2")]),
                )
            }
        ),
    ),
    (
        "SELECT '((1,2),(3,4))'::path",
        pa.table(
            {
                "path": pa.array(
                    [
                        {
                            "closed": True,
                            "points": [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}],
                        }
                    ]
                )
            }
        ),
    ),
    (
        "SELECT '[(1,2),(3,4)]'::path",
        pa.table(
            {
                "path": pa.array(
                    [
                        {
                            "closed": False,
                            "points": [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}],
                        }
                    ]
                )
            }
        ),
    ),
    (
        "SELECT '((1,2),(3,4))'::polygon",
        pa.table({"polygon": pa.array([[{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}]])}),
    ),
    (
        "SELECT '<(1.3, 3.4), 6>'::circle",
        pa.table(
            {
                "circle": pa.array(
                    [{"x": 1.3, "y": 3.4, "r": 6}],
                    pa.struct([(k, pa.float64()) for k in ("x", "y", "r")]),
                )
            }
        ),
    ),
    (
        "SELECT '::ffff:1.2.3.0/120'::cidr",
        pa.table(
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
        ),
    ),
    (
        "SELECT '192.168.100.128/25'::inet",
        pa.table(
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
        ),
    ),
    (
        "SELECT '08:00:2b:01:02:03'::macaddr",
        pa.table({"macaddr": pa.array([b"\x08\x00\x2B\x01\x02\x03"], pa.binary(6))}),
    ),
    (
        "SELECT '08:00:2b:01:02:03:04:05'::macaddr8",
        pa.table(
            {"macaddr8": pa.array([b"\x08\x00\x2B\x01\x02\x03\x04\x05"], pa.binary(8))}
        ),
    ),
    ("SELECT '{1000, 2000}'::bigint[]", pa.table({"int8": pa.array([[1000, 2000]])})),
]


@pytest.mark.parametrize("test", tests)
def test_query(dsn, test):
    query, expected = test
    tbl = copy_query(dsn, query)
    assert tbl.equals(expected)
