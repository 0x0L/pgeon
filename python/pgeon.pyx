# distutils: language=c++

from libcpp.memory cimport shared_ptr
from pyarrow.lib cimport import_pyarrow, CTable
from pyarrow cimport wrap_table

cdef extern from "pgeon.h" namespace "pgeon":
    cdef shared_ptr[CTable] CopyQuery(const char* conninfo, const char* query) nogil


import_pyarrow()


def copy_query(conninfo : str, query : str):
    enc_conninfo = conninfo.encode('utf8')
    enc_query = query.encode('utf8')

    cdef:
        shared_ptr[CTable] tbl
        const char* c_conninfo = enc_conninfo
        const char* c_query = enc_query

    with nogil:
        tbl = CopyQuery(c_conninfo, c_query)

    return wrap_table(tbl)
