# Pgeon 🐦

[![Build](https://github.com/0x0L/pgeon/actions/workflows/build.yml/badge.svg)](https://github.com/0x0L/pgeon/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/0x0L/pgeon/blob/main/LICENSE)

The fastest flight from [PostgreSQL](https://www.postgresql.org/) to [Apache Arrow](https://arrow.apache.org/).

`pgeon` provides a C++ library and (very) simple python bindings. Almost all
PostgreSQL native types are supported (see [below](#notes)).

This project is similar to [pg2arrow](https://github.com/heterodb/pg2arrow) and is heavily inspired by it. The main differences are the use of `COPY` instead of `FETCH` and that our implementation uses the Arrow C++ API.

## Install notes

To create a sensible environment you can do `conda env create -f environment.yml` or open the project in the dev container with VSCode.

```shell
git clone https://github.com/0x0L/pgeon.git
cd pgeon
pip install .
```

### [optional] Building the c++ library

```shell
mkdir build
cd build
cmake -GNinja ..
ninja
```

## Usage

```python
from pgeon import copy_query
db = "postgresql://postgres@localhost:5432/postgres"
db = "postgresql://localhost:5432/postgres"
tbl = copy_query(db, "SELECT TIMESTAMP '2001-01-01 14:00:00'")
print(tbl)
```

## Performance

Duration distributions from 100 consecutive runs of a query fetching 7 columns (1 timestamp, 2 ints, 4 reals)
and returning around 4.5 million rows.

![](benchmarks/minute_bars.svg)

The received `pyarrow.Table` can be further converted into a `pandas.DataFrame` in less than 25ms!

## Try it out

### Developer Notes

In order to create a sensible environment you can do `conda env create -f environment.yml` or open the project in the dev container with VSCode.

### Building the C++ library and programs

- `conda activate pgeon-dev`
- `mkdir build && cd build`
- `cmake -GNinja ..`

### Building the Python wrapper

In order to install the Python wrapper you can use standard Python tooling for packages, e.g.

- `pip install .`

Open the project in VS code dev container and build it. In the terminal, create a few sample tables with

```shell
sh tests/create_tables.sh
```

To test it out

```python
from pgeon import copy_query
db = "postgresql://postgres@localhost:5432/postgres"
tbl = copy_query(db, "select * from numeric_table")
print(tbl)
```

## Notes

- SQL arrays are mapped to `pyarrow.list_(...)`. Only 1D arrays are fully supported. Higher dimensional arrays will be flattened.

- BitString types output format is not really helpful

- tsvector types with letter weights are not supported

- PostgreSQL range and domain types are not supported.
