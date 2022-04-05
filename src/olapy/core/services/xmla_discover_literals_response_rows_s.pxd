from olapy.core.services.structures cimport RowTuples
from libcythonplus.list cimport cyplist


cdef cyplist[RowTuples] discover_literals_response_rows_l

cdef cyplist[RowTuples] _make_literals_response_rows() nogil
