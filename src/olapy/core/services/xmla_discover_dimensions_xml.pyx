from olapy.stdlib.string cimport Str
from olapy.stdlib.format cimport format
from olapy.cypxml.cypxml cimport Elem


cdef Elem fill_dimension_xml(
        Elem elem,
        Str catalog_name,
        Str tables,
        int ordinal,
        Str dimension_type,
        Str dimension_cardinal
    ) nogil:
    cdef Str br_tables

    br_tables = format("[{}]", tables)
    elem.stag("CATALOG_NAME").text(catalog_name)
    elem.stag("CUBE_NAME").text(catalog_name)
    elem.stag("DIMENSION_NAME").text(tables)
    elem.stag("DIMENSION_UNIQUE_NAME").text(br_tables)
    elem.stag("DIMENSION_CAPTION").text(tables)
    elem.stag("DIMENSION_ORDINAL").text(format("{}", ordinal))
    elem.stag("DIMENSION_TYPE").text(dimension_type)
    elem.stag("DIMENSION_CARDINALITY").text(dimension_cardinal)
    elem.stag("DEFAULT_HIERARCHY").text(format("{}.{}", br_tables, br_tables))
    elem.stag("IS_VIRTUAL").stext("false")
    elem.stag("IS_READWRITE").stext("false")
    elem.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
    elem.stag("DIMENSION_IS_VISIBLE").stext("true")


cdef Elem fill_dimension_measures_xml(
        Elem elem,
        Str catalog_name,
        int ordinal
    ) nogil:
    elem.stag("CATALOG_NAME").text(catalog_name)
    elem.stag("CUBE_NAME").text(catalog_name)
    elem.stag("DIMENSION_NAME").stext("Measures")
    elem.stag("DIMENSION_UNIQUE_NAME").stext("[Measures]")
    elem.stag("DIMENSION_CAPTION").stext("Measures")
    elem.stag("DIMENSION_ORDINAL").text(format("{}", ordinal))
    elem.stag("DIMENSION_TYPE").stext("2")
    elem.stag("DIMENSION_CARDINALITY").stext("0")
    elem.stag("DEFAULT_HIERARCHY").stext("[Measures]")
    elem.stag("IS_VIRTUAL").stext("false")
    elem.stag("IS_READWRITE").stext("false")
    elem.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
    elem.stag("DIMENSION_IS_VISIBLE").stext("true")
