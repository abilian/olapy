from olapy.core.services.structures cimport SchemaResponse


cdef SchemaResponse _make_MDSCHEMA_HIERARCHIES() nogil:
    cdef SchemaResponse row

    row = SchemaResponse()
    row.set_name("MDSCHEMA_HIERARCHIES")
    row.set_guid("C8B522DA-5CF3-11CE-ADE5-00AA0044773D")
    row.add_restriction("CATALOG_NAME", "string")
    row.add_restriction("SCHEMA_NAME", "string")
    row.add_restriction("CUBE_NAME", "string")
    row.add_restriction("DIMENSION_UNIQUE_NAME", "string")
    row.add_restriction("HIERARCHY_NAME", "string")
    row.add_restriction("HIERARCHY_UNIQUE_NAME", "string")
    row.add_restriction("HIERARCHY_ORIGIN", "unsignedShort")
    row.add_restriction("CUBE_SOURCE", "unsignedShort")
    row.add_restriction("HIERARCHY_VISIBILITY", "unsignedShort")
    row.set_restriction_mask("511")
    return row


cdef SchemaResponse _make_MDSCHEMA_MEASURES() nogil:
    cdef SchemaResponse row

    row = SchemaResponse()
    row.set_name("MDSCHEMA_MEASURES")
    row.set_guid("C8B522DC-5CF3-11CE-ADE5-00AA0044773D")
    row.add_restriction("CATALOG_NAME", "string")
    row.add_restriction("SCHEMA_NAME", "string")
    row.add_restriction("CUBE_NAME", "string")
    row.add_restriction("MEASURE_NAME", "string")
    row.add_restriction("MEASURE_UNIQUE_NAME", "string")
    row.add_restriction("MEASUREGROUP_NAME", "string")
    row.add_restriction("CUBE_SOURCE", "unsignedShort")
    row.add_restriction("MEASURE_VISIBILITY", "unsignedShort")
    row.set_restriction_mask("255")
    return row


cdef SchemaResponse _make_DBSCHEMA_TABLES() nogil:
    cdef SchemaResponse row

    row = SchemaResponse()
    row.set_name("DBSCHEMA_TABLES")
    row.set_guid("C8B52229-5CF3-11CE-ADE5-00AA0044773D")
    row.add_restriction("TABLE_CATALOG", "string")
    row.add_restriction("TABLE_SCHEMA", "string")
    row.add_restriction("TABLE_NAME", "string")
    row.add_restriction("TABLE_TYPE", "string")
    row.add_restriction("TABLE_OLAP_TYPE", "string")
    row.set_restriction_mask("31")
    return row


cdef SchemaResponse _make_DISCOVER_DATASOURCES() nogil:
    cdef SchemaResponse row

    row = SchemaResponse()
    row.set_name("DISCOVER_DATASOURCES")
    row.set_guid("06C03D41-F66D-49F3-B1B8-987F7AF4CF18")
    row.add_restriction("DataSourceName", "string")
    row.add_restriction("URL", "string")
    row.add_restriction("ProviderName", "string")
    row.add_restriction("ProviderType", "string")
    row.add_restriction("AuthenticationMode", "string")
    row.set_restriction_mask("31")
    return row


cdef SchemaResponse _make_DISCOVER_INSTANCES() nogil:
    cdef SchemaResponse row

    row = SchemaResponse()
    row.set_name("DISCOVER_INSTANCES")
    row.set_guid("20518699-2474-4C15-9885-0E947EC7A7E3")
    row.add_restriction("INSTANCE_NAME", "string")
    row.set_restriction_mask("1")
    return row


cdef SchemaResponse _make_DISCOVER_KEYWORDS() nogil:
    cdef SchemaResponse row

    row = SchemaResponse()
    row.set_name("DISCOVER_KEYWORDS")
    row.set_guid("1426C443-4CDD-4A40-8F45-572FAB9BBAA1")
    row.add_restriction("Keyword", "string")
    row.set_restriction_mask("1")
    return row


MDSCHEMA_HIERARCHIES_sr = _make_MDSCHEMA_HIERARCHIES()
MDSCHEMA_MEASURES_sr = _make_MDSCHEMA_MEASURES()
DBSCHEMA_TABLES_sr = _make_DBSCHEMA_TABLES()
DISCOVER_DATASOURCES_sr = _make_DISCOVER_DATASOURCES()
DISCOVER_INSTANCES_sr = _make_DISCOVER_INSTANCES()
DISCOVER_KEYWORDS_sr = _make_DISCOVER_KEYWORDS()
