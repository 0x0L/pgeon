# distutils: language=c++

from libcpp.memory cimport shared_ptr
from pyarrow.lib cimport import_pyarrow, CTable
from pyarrow cimport wrap_table

import_pyarrow()

cdef extern from "../src/sql.h" namespace "pgeon":
    cdef shared_ptr[CTable] GetTable(const char* conninfo, const char* query)

def Getoo(conninfo, query):
    cdef shared_ptr[CTable] tbl = GetTable(conninfo, query)
    return wrap_table(tbl)
