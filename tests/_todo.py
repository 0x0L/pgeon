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
