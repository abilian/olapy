from olapy.stdlib.string cimport Str
from libcythonplus.list cimport cyplist
from olapy.stdlib.format cimport format
from olapy.cypxml.cypxml cimport Elem
from olapy.core.services.utils cimport bracket, bracket2
from olapy.core.services.discover_properties_items cimport (
    properties_names,
    properties_captions,
    properties_datas
)

cdef void fill_dimension(Elem, Str, Str, int, Str, Str) nogil
cdef void fill_dimension_measures(Elem, Str, int) nogil
cdef void fill_cube(Elem, Str) nogil
cdef void fill_mds_measures(Elem, Str, Str) nogil
cdef void fill_mds_hier_table(Elem, Str, Str, Str) nogil
cdef void fill_mds_hier_name(Elem, Str, Str) nogil
cdef void _fill_mds_levels_base(Elem, Str, Str, Str, int, Str, Str) nogil
cdef void fill_mds_levels_table(Elem, Str, Str, Str, int) nogil
cdef void fill_mds_levels_measure(Elem, Str) nogil
cdef void fill_mds_measuregroups(Elem, Str) nogil
cdef void fill_mds_measuregroup_dimensions(Elem, Str, Str) nogil
cdef void fill_mds_properties(Elem, Str) nogil
cdef void fill_mds_members_a(Elem, Str, Str, Str, Str, Str) nogil
cdef void fill_mds_members_b(Elem, Str, Str, Str, Str, Str, Str, int, Str) nogil
