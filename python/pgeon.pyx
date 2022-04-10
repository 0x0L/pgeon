# distutils: language=c++

from libcpp.memory cimport shared_ptr
from pyarrow.lib cimport import_pyarrow, CTable
from pyarrow cimport wrap_table

from c_pgeon cimport GetTable as c_GetTable

import_pyarrow()

def GetTable(conninfo : str, query : str):
    enc_conninfo = conninfo.encode('utf8')
    enc_query = query.encode('utf8')

    cdef:
        shared_ptr[CTable] tbl
        const char* c_conninfo = enc_conninfo
        const char* c_query = enc_query

    with nogil:
        tbl = c_GetTable(c_conninfo, c_query)

    return wrap_table(tbl)
