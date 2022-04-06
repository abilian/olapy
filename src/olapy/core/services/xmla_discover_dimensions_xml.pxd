from olapy.stdlib.string cimport Str
from olapy.stdlib.format cimport format
from olapy.cypxml.cypxml cimport Elem


cdef Elem fill_dimension_xml(Elem, Str, Str, int, Str, Str) nogil
cdef Elem fill_dimension_measures_xml(Elem, Str, int) nogil
