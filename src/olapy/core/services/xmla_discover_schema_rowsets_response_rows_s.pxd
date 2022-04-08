from libcythonplus.list cimport cyplist
from olapy.core.services.structures cimport SchemaResponse
from olapy.core.services.xmla_discover_schema_rowsets_response_items cimport (
    _make_MDSCHEMA_HIERARCHIES,
    _make_MDSCHEMA_MEASURES
)


cdef cyplist[SchemaResponse] discover_schema_rowsets_response_rows_l
cdef cyplist[SchemaResponse] _make_schema_rowsets_response_rows() nogil
