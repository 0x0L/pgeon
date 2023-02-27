# distutils: language=c++
# cython: language_level=3
# cython: binding=True

from cython.operator cimport dereference as deref
from libcpp.memory cimport shared_ptr, unique_ptr

from pyarrow.lib cimport (
    _Weakrefable, c_bool, check_status, CStatus, CTable, move, pyarrow_wrap_table
)

import pyarrow

cdef extern from "pgeon.h" namespace "pgeon":
    cdef cppclass CUserOptions" pgeon::UserOptions":
        c_bool string_as_dictionaries
        int default_numeric_precision
        int default_numeric_scale
        int monetary_fractional_precision

        CUserOptions()
        CUserOptions(CUserOptions&&)

        @staticmethod
        CUserOptions Defaults()

        CStatus Validate()

    cdef shared_ptr[CTable] CopyQuery(const char* conninfo, const char* query, CUserOptions options) nogil


cdef class UserOptions(_Weakrefable):
    """Options

    Parameters
    ----------
    string_as_dictionaries : bool, optional (default False)
        Whether to treat string columns as dictionaries

    default_numeric_precision : int, optional (default 22)
        TODO

    default_numeric_scale : bool, optional (default 6)
        TODO

    monetary_fractional_precision : bool, optional (default 2)
        TODO
    """
    cdef:
        unique_ptr[CUserOptions] options

    __slots__ = ()

    def __cinit__(self, *argw, **kwargs):
        self.options.reset(
            new CUserOptions(CUserOptions.Defaults()))

    def __init__(self, *, string_as_dictionaries=None, default_numeric_precision=None,
        default_numeric_scale=None, monetary_fractional_precision=None):
        if string_as_dictionaries is not None:
            self.string_as_dictionaries = string_as_dictionaries
        if default_numeric_precision is not None:
            self.default_numeric_precision = default_numeric_precision
        if default_numeric_scale is not None:
            self.default_numeric_scale = default_numeric_scale
        if monetary_fractional_precision is not None:
            self.monetary_fractional_precision = monetary_fractional_precision

    @property
    def string_as_dictionaries(self):
        """
        Whether to treat string columns as dictionaries
        """
        return deref(self.options).string_as_dictionaries

    @string_as_dictionaries.setter
    def string_as_dictionaries(self, value):
        deref(self.options).string_as_dictionaries = value

    @property
    def default_numeric_precision(self):
        """
        TODO
        """
        return deref(self.options).default_numeric_precision

    @default_numeric_precision.setter
    def default_numeric_precision(self, value):
        deref(self.options).default_numeric_precision = value

    @property
    def default_numeric_scale(self):
        """
        TODO
        """
        return deref(self.options).default_numeric_scale

    @default_numeric_scale.setter
    def default_numeric_scale(self, value):
        deref(self.options).default_numeric_scale = value

    @property
    def monetary_fractional_precision(self):
        """
        TODO
        """
        return deref(self.options).monetary_fractional_precision

    @monetary_fractional_precision.setter
    def monetary_fractional_precision(self, value):
        deref(self.options).monetary_fractional_precision = value

    @staticmethod
    cdef UserOptions wrap(CUserOptions options):
        out = UserOptions()
        out.options.reset(new CUserOptions(move(options)))
        return out

    def validate(self):
        check_status(deref(self.options).Validate())

    def equals(self, UserOptions other):
        return (
            self.string_as_dictionaries == other.string_as_dictionaries and
            self.default_numeric_precision == other.default_numeric_precision and
            self.default_numeric_scale == other.default_numeric_scale and
            self.monetary_fractional_precision == other.monetary_fractional_precision
        )

    def __getstate__(self):
        return (self.string_as_dictionaries, self.default_numeric_precision,
            self.default_numeric_scale, self.monetary_fractional_precision)

    def __setstate__(self, state):
        (self.string_as_dictionaries, self.default_numeric_precision,
            self.default_numeric_scale, self.monetary_fractional_precision) = state

    def __eq__(self, other):
        try:
            return self.equals(other)
        except TypeError:
            return False


cdef _get_user_options(UserOptions user_options, CUserOptions* out):
    if user_options is None:
        out[0] = CUserOptions.Defaults()
    else:
        out[0] = deref(user_options.options)


def copy_query(conninfo : str, query : str, user_options: UserOptions=None) -> pyarrow.Table:
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
        CUserOptions c_options

    _get_user_options(user_options, &c_options)

    with nogil:
        tbl = CopyQuery(c_conninfo, c_query, c_options)

    return pyarrow_wrap_table(tbl)
