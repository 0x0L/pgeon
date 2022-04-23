# Pgeon 🐦

The fastest flight from [PostgreSQL](https://www.postgresql.org/) to [Apache Arrow](https://arrow.apache.org/).

## Performance

Kernel density estimates for 100 consecutive runs of a query fetching 7 columns (1 datetime, 2 ints, 4 reals)
and returning around 4.5 million rows:

![](benchmark.svg)

The received `pyarrow` table can be further converted into a `pandas` dataframe in less than 25ms!

## Try it out

Open the project in VS code Dev container.

After building the project, create a few sample tables with

```shell
cd /workspace/tests
sh create_tables.sh
```

To test it out
```shell
export LD_LIBRARY_PATH=/workspace/build
cd /workspace/python/build/lib.linux-x86_64-cpython-310

python <<EOF
import os
from pgeon import copy_query

connstr = os.environ["POSTGRES_CONN"]
req = "select * from numeric_table"

print(copy_query(connstr, req))
EOF
```

## TODO

* Tests & benchmarks

* Multi platform build / deploy

* Error handling

* Batchbuilder

* User options

    - control which strings (or columns) should be dict encoded. maybe as a default char varchar should be dict encoded and text should be large_utf8

    - review of bytes vs string and encoding

* Output format for bit(..)
