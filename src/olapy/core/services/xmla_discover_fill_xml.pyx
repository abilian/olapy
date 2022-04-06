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


cdef void fill_dimension(
        Elem elem,
        Str catalog_name,
        Str tables,
        int ordinal,
        Str dimension_type,
        Str dimension_cardinal
    ) nogil:
    elem.stag("CATALOG_NAME").text(catalog_name)
    elem.stag("CUBE_NAME").text(catalog_name)
    elem.stag("DIMENSION_NAME").text(tables)
    elem.stag("DIMENSION_UNIQUE_NAME").text(bracket(tables))
    elem.stag("DIMENSION_CAPTION").text(tables)
    elem.stag("DIMENSION_ORDINAL").text(format("{}", ordinal))
    elem.stag("DIMENSION_TYPE").text(dimension_type)
    elem.stag("DIMENSION_CARDINALITY").text(dimension_cardinal)
    elem.stag("DEFAULT_HIERARCHY").text(bracket2(tables))
    elem.stag("IS_VIRTUAL").stext("false")
    elem.stag("IS_READWRITE").stext("false")
    elem.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
    elem.stag("DIMENSION_IS_VISIBLE").stext("true")


cdef void fill_dimension_measures(
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


cdef void fill_cube(
        Elem elem,
        Str cube,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("CUBE_TYPE").stext("CUBE")
    elem.stag("LAST_SCHEMA_UPDATE").stext("2016-07-22T10:41:38")
    elem.stag("LAST_DATA_UPDATE").stext("2016-07-22T10:41:38")
    elem.stag("DESCRIPTION").text(format("MDX {} results", cube))
    elem.stag("IS_DRILLTHROUGH_ENABLED").stext("true")
    elem.stag("IS_LINKABLE").stext("false")
    elem.stag("IS_WRITE_ENABLED").stext("false")
    elem.stag("IS_SQL_ENABLED").stext("false")
    elem.stag("CUBE_CAPTION").text(cube)
    elem.stag("CUBE_SOURCE").stext("1")


cdef void fill_mds_measures(
        Elem elem,
        Str cube,
        Str measure,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("MEASURE_NAME").text(measure)
    elem.stag("MEASURE_UNIQUE_NAME").text(format("[Measures].[{}]", measure))
    elem.stag("MEASURE_CAPTION").text(measure)
    elem.stag("MEASURE_AGGREGATOR").stext("1")
    elem.stag("DATA_TYPE").stext("5")
    elem.stag("NUMERIC_PRECISION").stext("16")
    elem.stag("NUMERIC_SCALE").stext("-1")
    elem.stag("MEASURE_IS_VISIBLE").stext("true")
    elem.stag("MEASURE_NAME_SQL_COLUMN_NAME").text(measure)
    elem.stag("MEASURE_UNQUALIFIED_CAPTION").text(measure)
    elem.stag("MEASUREGROUP_NAME").stext("default")


cdef void fill_mds_hier_table(
        Elem elem,
        Str cube,
        Str table,
        Str column_attribut,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("DIMENSION_UNIQUE_NAME").text(bracket(table))
    elem.stag("HIERARCHY_NAME").text(table)
    elem.stag("HIERARCHY_UNIQUE_NAME").text(bracket2(table))
    elem.stag("HIERARCHY_CAPTION").text(table)
    elem.stag("DIMENSION_TYPE").stext("3")
    elem.stag("HIERARCHY_CARDINALITY").stext("6")
    if column_attribut is not NULL:
        elem.stag("ALL_MEMBER").text(format(
                                        "[{}].[{}].[{}]",
                                        table, table, column_attribut
                                    ))
    elem.stag("STRUCTURE").stext("0")
    elem.stag("IS_VIRTUAL").stext("false")
    elem.stag("IS_READWRITE").stext("false")
    elem.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
    elem.stag("DIMENSION_IS_VISIBLE").stext("true")
    elem.stag("HIERARCHY_ORDINAL").stext("1")
    elem.stag("DIMENSION_IS_SHARED").stext("true")
    elem.stag("HIERARCHY_IS_VISIBLE").stext("true")
    elem.stag("HIERARCHY_ORIGIN").stext("1")
    elem.stag("INSTANCE_SELECTION").stext("0")


cdef void fill_mds_hier_name(
        Elem elem,
        Str cube,
        Str default,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("DIMENSION_UNIQUE_NAME").stext("[Measures]")
    elem.stag("HIERARCHY_NAME").stext("Measures")
    elem.stag("HIERARCHY_UNIQUE_NAME").stext("[Measures]")
    elem.stag("HIERARCHY_CAPTION").stext("Measures")
    elem.stag("DIMENSION_TYPE").stext("2")
    elem.stag("HIERARCHY_CARDINALITY").stext("0")
    elem.stag("DEFAULT_MEMBER").text(format("[Measures].[{}]", default))
    elem.stag("STRUCTURE").stext("0")
    elem.stag("IS_VIRTUAL").stext("false")
    elem.stag("IS_READWRITE").stext("false")
    elem.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
    elem.stag("DIMENSION_IS_VISIBLE").stext("true")
    elem.stag("HIERARCHY_ORDINAL").stext("1")
    elem.stag("DIMENSION_IS_SHARED").stext("true")
    elem.stag("HIERARCHY_IS_VISIBLE").stext("true")
    elem.stag("HIERARCHY_ORIGIN").stext("1")
    elem.stag("INSTANCE_SELECTION").stext("0")


cdef void _fill_mds_levels_base(
        Elem elem,
        Str cube,
        Str table,
        Str column,
        int level,
        Str hier_unique_name,
        Str level_unique_name,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("DIMENSION_UNIQUE_NAME").text(bracket(table))
    elem.stag("HIERARCHY_UNIQUE_NAME").text(hier_unique_name)
    elem.stag("LEVEL_NAME").text(column)
    elem.stag("LEVEL_UNIQUE_NAME").text(level_unique_name)
    elem.stag("LEVEL_CAPTION").text(column)
    elem.stag("LEVEL_NUMBER").text(format("{}", level))
    elem.stag("LEVEL_CARDINALITY").stext("0")
    elem.stag("LEVEL_TYPE").stext("0")
    elem.stag("CUSTOM_ROLLUP_SETTINGS").stext("0")
    elem.stag("LEVEL_UNIQUE_SETTINGS").stext("0")
    elem.stag("LEVEL_IS_VISIBLE").stext("true")
    elem.stag("LEVEL_DBTYPE").stext("130")
    elem.stag("LEVEL_KEY_CARDINALITY").stext("1")
    elem.stag("LEVEL_ORIGIN").stext("2")


cdef void fill_mds_levels_table(
        Elem elem,
        Str cube,
        Str table,
        Str column,
        int level,
    ) nogil:
    cdef Str hier_unique_name
    cdef Str level_unique_name

    hier_unique_name = bracket2(table)
    level_unique_name = format("[{}].[{}].[{}]", table, table, column)
    _fill_mds_levels_base(
        elem, cube, table, column, level, hier_unique_name, level_unique_name)


cdef void fill_mds_levels_measure(
        Elem elem,
        Str cube,
    ) nogil:
    cdef Str table
    cdef Str column
    cdef Str unique_name

    table = Str("Measures")
    column = Str("MeasuresLevel")
    unique_name = bracket(table)
    _fill_mds_levels_base(elem, cube, table, column, 0, unique_name, unique_name)


cdef void fill_mds_measuregroups(
        Elem elem,
        Str cube,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("MEASUREGROUP_NAME").stext("default")
    elem.stag("DESCRIPTION").stext("-")
    elem.stag("IS_WRITE_ENABLED").stext("true")
    elem.stag("MEASUREGROUP_CAPTION").stext("default")


cdef void fill_mds_measuregroup_dimensions(
        Elem elem,
        Str cube,
        Str table,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("MEASUREGROUP_NAME").stext("default")
    elem.stag("MEASUREGROUP_CARDINALITY").stext("ONE")
    elem.stag("DIMENSION_UNIQUE_NAME").text(bracket(table))
    elem.stag("DIMENSION_CARDINALITY").stext("MANY")
    elem.stag("DIMENSION_IS_VISIBLE").stext("true")
    elem.stag("DIMENSION_IS_FACT_DIMENSION").stext("false")
    elem.stag("DIMENSION_GRANULARITY").text(bracket2(table))


cdef void fill_mds_properties(Elem root, Str cube) nogil:
    cdef size_t idx

    for idx in range(properties_names.__len__()):
        row = root.stag("row")
        row.stag("CATALOG_NAME").text(cube)
        row.stag("PROPERTY_TYPE").stext("2")
        row.stag("PROPERTY_NAME").text((properties_names[idx]))
        row.stag("PROPERTY_CAPTION").text((properties_captions[idx]))
        row.stag("DATA_TYPE").text((properties_datas[idx]))


cdef void fill_mds_members_a(
        Elem elem,
        Str cube,
        Str dim_unique_name,
        Str level_unique_name,
        Str member_name,
        Str member_level_name,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("DIMENSION_UNIQUE_NAME").text(dim_unique_name)
    elem.stag("HIERARCHY_UNIQUE_NAME").text(format("{}.{}",
                                                   dim_unique_name, dim_unique_name))
    elem.stag("LEVEL_UNIQUE_NAME").text(level_unique_name)
    elem.stag("LEVEL_NUMBER").stext("0")
    elem.stag("MEMBER_ORDINAL").stext("0")
    elem.stag("MEMBER_NAME").text(member_name)
    elem.stag("MEMBER_UNIQUE_NAME").text(member_level_name)
    elem.stag("MEMBER_TYPE").stext("1")
    elem.stag("MEMBER_CAPTION").text(member_name)
    elem.stag("CHILDREN_CARDINALITY").stext("1")
    elem.stag("PARENT_LEVEL").stext("0")
    elem.stag("PARENT_COUNT").stext("0")
    elem.stag("MEMBER_KEY").text(member_name)
    elem.stag("IS_PLACEHOLDERMEMBER").stext("false")
    elem.stag("IS_DATAMEMBER").stext("false")


cdef void fill_mds_members_b(
        Elem elem,
        Str cube,
        Str dim_unique_name,
        Str hier_unique_name,
        Str level_unique_name,
        Str member_name,
        Str member_level_name,
        int level_number,
        Str parent_unique_name,
    ) nogil:
    elem.stag("CATALOG_NAME").text(cube)
    elem.stag("CUBE_NAME").text(cube)
    elem.stag("DIMENSION_UNIQUE_NAME").text(dim_unique_name)
    elem.stag("HIERARCHY_UNIQUE_NAME").text(hier_unique_name)
    elem.stag("LEVEL_UNIQUE_NAME").text(level_unique_name)
    elem.stag("LEVEL_NUMBER").text(format("{}", level_number))
    elem.stag("MEMBER_ORDINAL").stext("0")
    elem.stag("MEMBER_NAME").text(member_name)
    elem.stag("MEMBER_UNIQUE_NAME").text(member_level_name)
    elem.stag("MEMBER_TYPE").stext("1")
    elem.stag("MEMBER_CAPTION").text(member_name)
    elem.stag("CHILDREN_CARDINALITY").stext("1")
    elem.stag("PARENT_LEVEL").stext("0")
    elem.stag("PARENT_COUNT").stext("0")
    elem.stag("PARENT_UNIQUE_NAME").text(parent_unique_name)
    elem.stag("MEMBER_KEY").text(member_name)
    elem.stag("IS_PLACEHOLDERMEMBER").stext("false")
    elem.stag("IS_DATAMEMBER").stext("false")
