# We need the following line before importing anything from _pgeon
# in order to preload libarrow.so.*
import pyarrow as _pa  # noqa

from ._pgeon import copy_query

__all__ = ["copy_query"]
