import pytest

from pgeon import copy_query

queries = [
    """
    SELECT
    x::decimal AS decimal_round,
    x::numeric AS numeric_round,
    x::smallint AS smallint_round,
    x::integer AS integer_round,
    x::bigint AS bigint_round,
    x::real AS real_round,
    x::double precision AS dbl_round
    FROM generate_series(-3.5, 3.5, 1) AS x
    """,
    "SELECT * from numeric_table",
    "SELECT '12.3342'::money",
    "SELECT '120'::money",
    "SELECT 'ok'::character(4)",
    "SELECT 'too long'::varchar(5)",
    "SELECT 'b'::char",
    "SELECT 'b'::name",
    "SELECT 'abc \153\154\155 \052\251\124'::bytea",
    r"SELECT '\xDEADBEEF'",
    "SELECT TIMESTAMP '2001-01-01 14:00:00'",
    "SELECT TIMESTAMP WITH TIME ZONE '2001-01-01 14:00:00+02:00'",
    "SELECT '1999-01-08'::date",
    "SELECT TIME '14:00:00'",
    "SELECT TIME WITH TIME ZONE '14:00:00+02:00'",
    "SELECT '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval",
    "SELECT 'true'::boolean",
    "SELECT 'false'::boolean",
    "SELECT * from person",
    "SELECT '(1.2, 4.3)'::point",
    "SELECT '{1.2, 4.3, 0.0}'::line",
    "SELECT '((1.2, 4.3), (5.6, 7.8))'::lseg",
    "SELECT '((1.2, 4.3), (5.6, 7.8))'::box",
    "SELECT '((1,2),(3,4))'::path",
    "SELECT '[(1,2),(3,4)]'::path",
    "SELECT '((1,2),(3,4))'::polygon",
    "SELECT '<(1.3, 3.4), 6>'::circle",
    "SELECT '::ffff:1.2.3.0/120'::cidr",
    "SELECT '192.168.100.128/25'::inet",
    "SELECT '08:00:2b:01:02:03'::macaddr",
    "SELECT '08:00:2b:01:02:03:04:05'::macaddr8",
    "SELECT B'101'::bit(3)",
    "SELECT B'10'::bit varying(5)",
    "SELECT 'a fat cat sat on a mat and ate a fat rat'::tsvector",
    "SELECT $$the lexeme '    ' contains spaces$$::tsvector",
    "SELECT $$the lexeme 'Joe''s' contains a quote$$::tsvector",
    "SELECT 'a:1 fat:2 cat:3 sat:4 on:5 a:6 mat:7 and:8 ate:9 a:10 fat:11 rat:12'::tsvector",
    "SELECT 'a:1A fat:2B,4C cat:5D'::tsvector",
    "SELECT 'The Fat Rats'::tsvector",
    "SELECT to_tsvector('english', 'The Fat Rats')",
    "SELECT 'fat & rat'::tsquery",
    "SELECT 'fat & (rat | cat)'::tsquery",
    "SELECT 'fat & rat & ! cat'::tsquery",
    "SELECT 'fat:ab & cat'::tsquery",
    "SELECT 'super:*'::tsquery",
    "SELECT to_tsquery('Fat:ab & Cats')",
    "SELECT to_tsvector( 'postgraduate' ) @@ to_tsquery( 'postgres:*' )",
    "SELECT to_tsvector( 'postgraduate' ), to_tsquery( 'postgres:*' )",
    "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid",
    """XMLPARSE (DOCUMENT '<?xml version="1.0"?><book><title>Manual</title><chapter>...</chapter></book>')""",
    "XMLPARSE (CONTENT 'abc<foo>bar</foo><bar>foo</bar>')",
    "SELECT * from json_table",
    "SELECT (a_jsonb->>'a')::int from json_table",
    "SELECT '$.key'::jsonpath",
    "SELECT * from sal_emp",
    # Composite types
    # arrays are flattened
    "SELECT * from on_hand",
    "SELECT int4range(10, 20)",
    "SELECT '{[3,7), [8,9)}'::int4multirange",
    "SELECT int8range(10, 20)",
    "SELECT numrange(11.1, 22.2)",
    "SELECT nummultirange(numrange(1.0, 14.0), numrange(20.0, 25.0))",
    "SELECT * FROM pg_attribute WHERE attrelid = 'on_hand'::regclass",
    "SELECT pg_current_snapshot()",
    "SELECT 'NULL'::void",
    "SELECT 'a=>1,b=>2'::hstore",
    "SELECT 'a=>1,a=>2'::hstore",
    """SELECT 'a=>1,b=>""'::hstore""",
    """SELECT 'a=>1,b=>NULL'::hstore""",
    "SELECT ROW('1'::real, '2'::real)",
]


@pytest.mark.parametrize("query", queries)
def test_query(conninfo, query):
    tbl = copy_query(conninfo, query)
    print(tbl)
