from olapy.core.services.structures cimport SchemaResponse


cdef SchemaResponse MDSCHEMA_HIERARCHIES_sr
cdef SchemaResponse MDSCHEMA_MEASURES_sr
cdef SchemaResponse DBSCHEMA_TABLES_sr
cdef SchemaResponse DISCOVER_DATASOURCES_sr
cdef SchemaResponse DISCOVER_INSTANCES_sr
cdef SchemaResponse DISCOVER_KEYWORDS_sr


cdef SchemaResponse _make_MDSCHEMA_HIERARCHIES() nogil
cdef SchemaResponse _make_MDSCHEMA_MEASURES() nogil
cdef SchemaResponse _make_DBSCHEMA_TABLES() nogil
cdef SchemaResponse _make_DISCOVER_DATASOURCES() nogil
cdef SchemaResponse _make_DISCOVER_INSTANCES() nogil
cdef SchemaResponse _make_DISCOVER_KEYWORDS() nogil
