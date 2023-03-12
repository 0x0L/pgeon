# Pgeon 🐦

[![Build](https://github.com/0x0L/pgeon/actions/workflows/build.yml/badge.svg)](https://github.com/0x0L/pgeon/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/0x0L/pgeon/blob/main/LICENSE)

[Apache Arrow](https://arrow.apache.org/) [PostgreSQL](https://www.postgresql.org/) connector

The goal of `pgeon` is to provide fast bulk data download from a PostgreSQL database into Apache Arrow tables. `pgeon` provides a C++ library and simple python bindings. Almost all PostgreSQL native types are supported (see [below](#notes)).

If you're looking to upload data, you might want to have a look at [Arrow ADBC](https://github.com/apache/arrow-adbc).

This project is similar to [pg2arrow](https://github.com/heterodb/pg2arrow) and is heavily inspired by it. The main differences are the use of `COPY` instead of `FETCH` and that our implementation uses the Arrow C++ API.

## Usage

```python
from pgeon import copy_query
db = "postgresql://postgres@localhost:5432/postgres"
query = "SELECT * FROM some_table"
tbl = copy_query(db, query)
```

The actual query performed is `COPY ({query}) TO STDOUT (FORMAT binary)`, see [this page](https://www.postgresql.org/docs/current/sql-copy.html) for more information.

## Installation

### Pre-built binary wheels

We provide pre-built binary wheels in the [Release](https://github.com/0x0L/pgeon/releases) section. No dependencies are required. Conda users, please read below.

### Install from sources

Building `pgeon` requires [libpq](https://www.postgresql.org/docs/current/libpq.html) to be available on your system.

```shell
git clone https://github.com/0x0L/pgeon.git
cd pgeon
pip install .
```

The pre-built binary wheels are built using the old C++ ABI as used by the `pyarrow` package available from [pypi](https://pypi.org/project/pyarrow/). Unfortunately the conda-forge `pyarrow` package uses the new C++ ABI. If you are using `pyarrow` from conda at runtime, you can install `pgeon` using

```shell
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

- SQL arrays are mapped to `pyarrow.list_(...)`. Due to the PostgreSQL wire format, only 1D arrays are fully supported. Higher dimensional arrays will be flattened.

- BitString types output format is not really helpful

- tsvector types with letter weights are not supported

- PostgreSQL range and domain types are not supported

- Dynamic record types are not supported
