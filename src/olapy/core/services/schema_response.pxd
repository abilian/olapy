from olapy.stdlib.string cimport Str
from libcythonplus.list cimport cyplist
from olapy.core.services.structures cimport SchemaResponse


cdef Str discover_schema_rowsets_xsd_s

cdef Str discover_schema_rowsets_response_str(cyplist[SchemaResponse])
