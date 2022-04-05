from olapy.stdlib.string cimport Str
from olapy.stdlib.format cimport format
from olapy.cypxml cimport cypXML
from olapy.cypxml.cypxml cimport Elem
from libc.stdlib  cimport getenv


cdef Str username() nogil
cdef Elem _fill_properties(Elem, Str, Str, Str, Str, Str, Str) nogil
cdef Str properties_xml(Str, Str, Str, Str, Str, Str, Str) nogil
