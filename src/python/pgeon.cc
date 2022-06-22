#include "pgeon.h"
#include <arrow/python/pyarrow.h>
#include <pybind11/pybind11.h>

static pybind11::handle copy_query(const char* conninfo, const char* query) {
  return arrow::py::wrap_table(pgeon::CopyQuery(conninfo, query));
}

PYBIND11_MODULE(_pgeon, m) {
  arrow::py::import_pyarrow();

  m.doc() =
      "The fastest flight from PostgreSQL to Apache Arrow";  // optional module docstring

  m.def("copy_query", &copy_query, "Run a COPY query against the db");
}
