import psycopg
from pgeon import copy_query

db = "postgresql://localhost/mytests"

with psycopg.connect(db) as conn:
    # Open a cursor to perform database operations
    with conn.cursor() as cur:

        # Execute a command: this creates a new table
        cur.execute("DROP TABLE IF EXISTS test")

        cur.execute("""
            CREATE TABLE test (
                id serial PRIMARY KEY,
                num integer,
                data text)
            """)

        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no SQL injections!)
        cur.execute(
            "INSERT INTO test (num, data) VALUES (%s, %s)",
            (100, "abc'def"))

        conn.commit()

        tbl = copy_query(db, "SELECT * from test")
        print(tbl)

        # Query the database and obtain data as Python objects.
        # cur.execute("select * from test", binary=True)
        # s = cur.fetchall()
        # print(s[0])
        # print(s)
        # s = cur.fetchone()
        # print(s)
        # will return (1, 100, "abc'def")

        # You can use `cur.fetchmany()`, `cur.fetchall()` to return a list
        # of several records, or even iterate on the cursor
        # for record in cur:
        #     print(record)

        # Make the changes to the database persistent
        # conn.commit()



# with psycopg.connect(db) as conn:
#     u = []
#     with conn.cursor().copy("COPY minute_bars TO STDOUT (FORMAT BINARY)") as copy:
#         for v in copy:
#             u.append(v)

#     print(len(u))
