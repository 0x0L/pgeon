from pgeon import copy_query

db = "postgresql://localhost/mytests"
# query = "select * from minute_bars"
query = "select 'a'::char"
tbl = copy_query(db, query)
print(tbl)
