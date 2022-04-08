from olapy.stdlib.string cimport Str
from olapy.cypxml cimport cypXML, Elem
from olapy.core.services.utils cimport username


cdef Elem _fill_properties(Elem, Str, Str, Str, Str, Str, Str) nogil
cdef Str properties_xml(Str, Str, Str, Str, Str, Str, Str) nogil
