# distutils: language=c++
# cython: language_level=3
# cython: binding=True

from libcpp.memory cimport shared_ptr
from pyarrow.lib cimport CTable, pyarrow_wrap_table

import pyarrow

cdef extern from "pgeon.h" namespace "pgeon":
    cdef shared_ptr[CTable] CopyQuery(const char* conninfo, const char* query) nogil


def copy_query(conninfo : str, query : str) -> pyarrow.Table:
    """Perform a query using the COPY interface

    Parameters
    ----------
    conninfo : str
        Connection string

    query : str
        The SQL query

    Returns
    -------
    pyarrow.Table
        The query result
    """
    enc_conninfo = conninfo.encode('utf8')
    enc_query = query.encode('utf8')

    cdef:
        shared_ptr[CTable] tbl
        const char* c_conninfo = enc_conninfo
        const char* c_query = enc_query

    with nogil:
        tbl = CopyQuery(c_conninfo, c_query)

    return pyarrow_wrap_table(tbl)
