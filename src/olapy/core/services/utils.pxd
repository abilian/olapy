from libc.stdlib  cimport getenv
from libcythonplus.list cimport cyplist
from olapy.stdlib.string cimport Str
from olapy.stdlib.format cimport format


cdef Str username() nogil
cdef Str bracket(Str) nogil
cdef Str bracket2(Str) nogil
cdef Str dot_bracket(cyplist[Str]) nogil
cdef cyplist[Str] pylist_to_cyplist(list)
cdef cyplist[Str] cypstr_copy_slice(cyplist[Str], size_t, size_t) nogil
cdef cyplist[Str] cypstr_copy_slice_from(cyplist[Str], size_t) nogil
cdef cyplist[Str] cypstr_copy_slice_to(cyplist[Str], size_t) nogil
