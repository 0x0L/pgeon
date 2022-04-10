# distutils: language=c++

from libcpp.memory cimport shared_ptr
from pyarrow.lib cimport CTable

cdef extern from "../src/sql.h" namespace "pgeon":
    cdef shared_ptr[CTable] GetTable(const char* conninfo, const char* query) nogil
