from olapy.stdlib.string cimport Str
from libcythonplus.list cimport cyplist

cdef cyplist[Str] properties_names
cdef cyplist[Str] properties_captions
cdef cyplist[Str] properties_datas
cdef cyplist[Str] _make_properties_names() nogil
cdef cyplist[Str] _make_properties_captions() nogil
cdef cyplist[Str] _make_properties_datas() nogil
