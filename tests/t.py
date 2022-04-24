import os
from pgeon import copy_query

db = os.environ["PGEON_TEST_DB"]

# Numeric Types
query = """
SELECT
  x::decimal AS decimal_round,
  x::numeric AS numeric_round,
  x::smallint AS smallint_round,
  x::integer AS integer_round,
  x::bigint AS bigint_round,
  x::real AS real_round,
  x::double precision AS dbl_round
FROM generate_series(-3.5, 3.5, 1) AS x
"""
query = "SELECT * from numeric_table"

# TODO parameteric test for decimal

# Monetary types
query = "SELECT '12.3342'::money"
query = "SELECT '120'::money"

# Character types
query = "SELECT 'ok'::character(4)"
query = "SELECT 'too long'::varchar(5)"
query = "SELECT 'b'::char"  # TODO
query = "SELECT 'b'::name"  # TODO

# TODO varchar(n) char(n) text
# TODO internal char (1 byte), name (64 bytes)

# Binary data types
query = "SELECT 'abc \153\154\155 \052\251\124'::bytea"
query = r"SELECT '\xDEADBEEF'"
# TODO check bytea	1 or 4 bytes plus the actual binary string variable-length binary string

# Date/Time types
query = "SELECT TIMESTAMP '2001-01-01 14:00:00'"
query = "SELECT TIMESTAMP WITH TIME ZONE '2001-01-01 14:00:00+02:00'"
query = "SELECT '1999-01-08'::date"
query = "SELECT TIME '14:00:00'"
query = "SELECT TIME WITH TIME ZONE '14:00:00+02:00'"
query = "SELECT '1 year 2 months 3 days 4 hours 5 minutes 6 seconds'::interval"

# Boolean type
query = "SELECT 'true'::boolean"
query = "SELECT 'false'::boolean"

# Enumerated types
query = "SELECT * from person"

# Geometric types
query = "SELECT '(1.2, 4.3)'::point"
query = "SELECT '{1.2, 4.3, 0.0}'::line"
query = "SELECT '((1.2, 4.3), (5.6, 7.8))'::lseg"
query = "SELECT '((1.2, 4.3), (5.6, 7.8))'::box"
query = "SELECT '((1,2),(3,4))'::path"  # strangely that's closed
query = "SELECT '[(1,2),(3,4)]'::path"  # open
query = "SELECT '((1,2),(3,4))'::polygon"
query = "SELECT '<(1.3, 3.4), 6>'::circle"

# Network address types
query = "SELECT '::ffff:1.2.3.0/120'::cidr"
query = "SELECT '192.168.100.128/25'::inet"
query = "SELECT '08:00:2b:01:02:03'::macaddr"
query = "SELECT '08:00:2b:01:02:03:04:05'::macaddr8"

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

# UUID type
query = "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid"

# XML type
query = """XMLPARSE (DOCUMENT '<?xml version="1.0"?><book><title>Manual</title><chapter>...</chapter></book>')"""
query = "XMLPARSE (CONTENT 'abc<foo>bar</foo><bar>foo</bar>')"

# JSON types
query = "SELECT * from json_table"
query = "SELECT (a_jsonb->>'a')::int from json_table"
query = "SELECT '$.key'::jsonpath"

# Arrays
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
query = "SELECT * FROM pg_attribute WHERE attrelid = 'on_hand'::regclass"  # TODO segfault

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

tbl = copy_query(db, query)
print(tbl)
