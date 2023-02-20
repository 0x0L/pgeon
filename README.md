# Pgeon 🐦

[![Build](https://github.com/0x0L/pgeon/actions/workflows/build.yml/badge.svg)](https://github.com/0x0L/pgeon/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/0x0L/pgeon/blob/main/LICENSE)

[Apache Arrow](https://arrow.apache.org/) [PostgreSQL](https://www.postgresql.org/) connector

`pgeon` provides a C++ library and (very) simple python bindings. Almost all
PostgreSQL native types are supported (see [below](#notes)).

This project is similar to [pg2arrow](https://github.com/heterodb/pg2arrow) and is heavily inspired by it. The main differences are the use of `COPY` instead of `FETCH` and that our implementation uses the Arrow C++ API.

The goal of `pgeon` is to provide fast bulk data download from a PostgreSQL database into Apache Arrow tables. If you're looking to upload data, you might want to have a look at [Arrow ADBC](https://github.com/apache/arrow-adbc).

## Usage

```python
from pgeon import copy_query
db = "postgresql://postgres@localhost:5432/postgres"
query = "SELECT TIMESTAMP '2001-01-01 14:00:00'"
tbl = copy_query(db, query)
```

The actual query performed is `COPY ({query}) TO STDOUT (FORMAT binary)`, see [this page](https://www.postgresql.org/docs/current/sql-copy.html) for more information.

## Installation

Building and running `pgeon` requires [libpq](https://www.postgresql.org/docs/current/libpq.html) to be available on your system.

### Python

Install from source using pip with

```shell
git clone https://github.com/0x0L/pgeon.git
cd pgeon
pip install .
```

On linux, if `pyarrow` is already installed as a conda package, you may want to use

```
CONDA_BUILD=1 pip install .
```

### [optional] C++ library and tools

This requires [cmake](https://cmake.org/) and [ninja](https://ninja-build.org/). In addition you'll need to install `libpq` and the Arrow C++ libraries (e.g. `arrow-cpp` in conda)

```shell
mkdir build
cd build
cmake -GNinja ..
ninja
```

## Performance

Elapsed time distributions of a query fetching 7 columns (1 timestamp, 2 ints, 4 reals) and around 4.5 million rows. The result is returned as a `pandas.DataFrame` in all cases.

![](benchmarks/minute_bars.svg)

## Notes

- Queries using `ROW` (e.g. `SELECT ROW('a', 1)`) do not work (anonymous structs)

- SQL arrays are mapped to `pyarrow.list_(...)`. Only 1D arrays are fully supported. Higher dimensional arrays will be flattened.

- BitString types output format is not really helpful

- tsvector types with letter weights are not supported

- PostgreSQL range and domain types are not supported.

