# distutils: language=c++

from libcpp.memory cimport shared_ptr
from pyarrow.lib cimport import_pyarrow, CTable
from pyarrow cimport wrap_table
from pyarrow.lib import tobytes

import_pyarrow()

cdef extern from "../src/sql.h" namespace "pgeon":
    cdef shared_ptr[CTable] GetTable(const char* conninfo, const char* query) nogil

def Getoo(conninfo, query):
    cdef:
        shared_ptr[CTable] tbl
        const char* c_conninfo = conninfo
        const char* c_query = query

    with nogil:
        tbl = GetTable(c_conninfo, c_query)

    return wrap_table(tbl)
