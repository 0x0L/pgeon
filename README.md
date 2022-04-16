# Pgeon 🐦

Fast data retrieval from a PostgreSQL database into Apache Arrow format.

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
from pgeon import get_table

connstr = os.environ["POSTGRES_CONN"]
req = "select * from numeric_table"

print(get_table(connstr, req))
EOF
```

## TODO

* Tests

* Benchmarks

* User options

    - control which strings (or columns) should be dict encoded

* Global refactor / cleanup / style / build

* Error handling

* Batchbuilder
