"""
Managing all `DISCOVER <https://technet.microsoft.com/fr-fr/library/ms186653(v=sql.110).aspx>`_ requests and responses
"""
# -*- encoding: utf8 -*-

from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os

import xmlwitch
from six.moves.urllib.parse import urlparse

try:
    from sqlalchemy import create_engine
except ImportError:
    pass

from ..services.dict_discover_request_handler import DictDiscoverReqHandler
from ..services.xmla_discover_request_utils import discover_literals_response_rows, \
    discover_schema_rowsets_response_rows
from .xmla_discover_xsds import dbschema_catalogs_xsd, dbschema_tables_xsd, \
    discover_datasources_xsd, discover_literals_xsd, \
    discover_schema_rowsets_xsd, mdschema_cubes_xsd, mdschema_dimensions_xsd, \
    mdschema_hierarchies_xsd, mdschema_kpis_xsd, mdschema_levels_xsd, \
    mdschema_measures_xsd, mdschema_measuresgroups_dimensions_xsd, \
    mdschema_measuresgroups_xsd, mdschema_members_xsd, \
    mdschema_properties_properties_xsd, mdschema_sets_xsd

# noinspection PyPep8Naming


class XmlaDiscoverReqHandler(DictDiscoverReqHandler):
    """XmlaDiscoverReqHandler handles information, such as the list of available databases or details about a \
    specific object (cube, dimensions, hierarchies...), from an instance of MdxEngine. The data retrieved with the \
    Discover method depends on the values of the parameters passed to it. ."""

    def _change_db_uri(self, old_sqla_uri, new_db):
        # scheme, netloc, path, params, query, fragment = urlparse(old_sqla_uri)
        # urlunparse((scheme, netloc, new_db, params, query, fragment))
        # urlunparse -> bad result with sqlite://
        parse_uri = urlparse(old_sqla_uri)
        return parse_uri.scheme + "://" + parse_uri.netloc + "/" + new_db

    def change_cube(self, new_cube):
        """
        If you change the cube in any request, we have to
        instantiate the MdxEngine with the new cube.

        :param new_cube: cube name
        :return: new instance of MdxEngine with new star_schema_DataFrame and other variables
        """
        if self.selected_cube != new_cube:
            if (self.executor.cube_config
                    and new_cube == self.executor.cube_config["name"]):
                facts = self.executor.cube_config["facts"]["table_name"]
            else:
                facts = "Facts"

            self.selected_cube = new_cube
            if "db" in self.executor.source_type:
                new_sql_alchemy_uri = self._change_db_uri(
                    self.sql_alchemy_uri,
                    new_cube,
                )
                self.executor.sqla_engine = create_engine(new_sql_alchemy_uri)
            if self.executor.cube != new_cube:
                self.executor.load_cube(new_cube, fact_table_name=facts)

    @staticmethod
    def discover_datasources_response():
        """
        list of data sources that are available on the server
        :return:
        """
        xml = xmlwitch.Builder()
        with xml["return"]:
            with xml.root(
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        "xmlns:EX":
                        "urn:schemas-microsoft-com:xml-analysis:exception",
                        "xmlns:xsd":
                        "http://www.w3.org/2001/XMLSchema",
                        "xmlns:xsi":
                        "http://www.w3.org/2001/XMLSchema-instance",
                    }):
                xml.write(discover_datasources_xsd)
                with xml.row:
                    xml.DataSourceName("sales")
                    xml.DataSourceDescription("sales Sample Data")
                    xml.URL("http://127.0.0.1:8000/xmla")
                    xml.DataSourceInfo("-")
                    xml.ProviderName("olapy")
                    xml.ProviderType("MDP")
                    xml.AuthenticationMode("Unauthenticated")

        return str(xml)

    @staticmethod
    def _get_props(
            xsd,
            PropertyName,
            PropertyDescription,
            PropertyType,
            PropertyAccessType,
            IsRequired,
            Value,
    ):
        xml = xmlwitch.Builder()

        if PropertyName is not "":
            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(xsd)
                    with xml.row:
                        xml.PropertyName(PropertyName)
                        xml.PropertyDescription(PropertyDescription)
                        xml.PropertyType(PropertyType)
                        xml.PropertyAccessType(PropertyAccessType)
                        xml.IsRequired(IsRequired)
                        xml.Value(Value)

        else:
            properties_names_n_description = [
                "ServerName",
                "ProviderVersion",
                "MdpropMdxSubqueries",
                "MdpropMdxDrillFunctions",
                "MdpropMdxNamedSets",
            ]
            properties_types = ["string", "string", "int", "int", "int"]
            values = [
                os.getenv("USERNAME", "default"),
                "0.0.3  25-Nov-2016 07:20:28 GMT",
                "15",
                "3",
                "15",
            ]

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(xsd)
                    for idx, prop_desc in enumerate(
                            properties_names_n_description, ):
                        with xml.row:
                            xml.PropertyName(prop_desc)
                            xml.PropertyDescription(prop_desc)
                            xml.PropertyType(properties_types[idx])
                            xml.PropertyAccessType("Read")
                            xml.IsRequired("false")
                            xml.Value(values[idx])

        return str(xml)

    def discover_schema_rowsets_response(self, request):
        """
        Generate the names, restrictions, description, and other information for all enumeration values and any \
        additional provider-specific enumeration values supported by OlaPy.
        :param request:
        :return: xmla response as string
        """
        rows = discover_schema_rowsets_response_rows

        def generate_resp(rows):
            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(discover_schema_rowsets_xsd)
                    for resp_row in rows:
                        with xml.row:
                            xml.SchemaName(resp_row["SchemaName"])
                            xml.SchemaGuid(resp_row["SchemaGuid"])
                            for idx, restriction in enumerate(resp_row["restrictions"]["restriction_names"], ):
                                with xml.Restrictions:
                                    xml.Name(restriction)
                                    xml.Type(
                                        resp_row["restrictions"]
                                        ["restriction_types"][idx], )

                            xml.RestrictionsMask(resp_row["RestrictionsMask"])

            return str(xml)

        restriction_list = request.Restrictions.RestrictionList
        if (restriction_list.SchemaName == "MDSCHEMA_HIERARCHIES"
                and request.Properties.PropertyList.Catalog is not None):
            self.change_cube(request.Properties.PropertyList.Catalog)

            restriction_names = [
                "CATALOG_NAME",
                "SCHEMA_NAME",
                "CUBE_NAME",
                "DIMENSION_UNIQUE_NAME",
                "HIERARCHY_NAME",
                "HIERARCHY_UNIQUE_NAME",
                "HIERARCHY_ORIGIN",
                "CUBE_SOURCE",
                "HIERARCHY_VISIBILITY",
            ]
            restriction_types = [
                "string",
                "string",
                "string",
                "string",
                "string",
                "string",
                "unsignedShort",
                "unsignedShort",
                "unsignedShort",
            ]

            rows = [
                {
                    "SchemaName": "MDSCHEMA_HIERARCHIES",
                    "SchemaGuid": "C8B522DA-5CF3-11CE-ADE5-00AA0044773D",
                    "restrictions": {
                        "restriction_names": restriction_names,
                        "restriction_types": restriction_types,
                    },
                    "RestrictionsMask": "511",
                },
            ]

            return generate_resp(rows)

        if (restriction_list.SchemaName == "MDSCHEMA_MEASURES"
                and request.Properties.PropertyList.Catalog is not None):
            self.change_cube(request.Properties.PropertyList.Catalog)

            restriction_names = [
                "CATALOG_NAME",
                "SCHEMA_NAME",
                "CUBE_NAME",
                "MEASURE_NAME",
                "MEASURE_UNIQUE_NAME",
                "MEASUREGROUP_NAME",
                "CUBE_SOURCE",
                "MEASURE_VISIBILITY",
            ]
            restriction_types = [
                "string",
                "string",
                "string",
                "string",
                "string",
                "string",
                "unsignedShort",
                "unsignedShort",
            ]

            rows = [
                {
                    "SchemaName": "MDSCHEMA_MEASURES",
                    "SchemaGuid": "C8B522DC-5CF3-11CE-ADE5-00AA0044773D",
                    "restrictions": {
                        "restriction_names": restriction_names,
                        "restriction_types": restriction_types,
                    },
                    "RestrictionsMask": "255",
                },
            ]

            return generate_resp(rows)

        ext = [
            {
                "SchemaName": "DBSCHEMA_TABLES",
                "SchemaGuid": "C8B52229-5CF3-11CE-ADE5-00AA0044773D",
                "restrictions": {
                    "restriction_names": [
                        "TABLE_CATALOG",
                        "TABLE_SCHEMA",
                        "TABLE_NAME",
                        "TABLE_TYPE",
                        "TABLE_OLAP_TYPE",
                    ],
                    "restriction_types": [
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                    ],
                },
                "RestrictionsMask": "31",
            },
            {
                "SchemaName": "DISCOVER_DATASOURCES",
                "SchemaGuid": "06C03D41-F66D-49F3-B1B8-987F7AF4CF18",
                "restrictions": {
                    "restriction_names": [
                        "DataSourceName",
                        "URL",
                        "ProviderName",
                        "ProviderType",
                        "AuthenticationMode",
                    ],
                    "restriction_types": [
                        "string",
                        "string",
                        "string",
                        "string",
                        "string",
                    ],
                },
                "RestrictionsMask": "31",
            },
            {
                "SchemaName": "DISCOVER_INSTANCES",
                "SchemaGuid": "20518699-2474-4C15-9885-0E947EC7A7E3",
                "restrictions": {
                    "restriction_names": ["INSTANCE_NAME"],
                    "restriction_types": ["string"],
                },
                "RestrictionsMask": "1",
            },
            {
                "SchemaName": "DISCOVER_KEYWORDS",
                "SchemaGuid": "1426C443-4CDD-4A40-8F45-572FAB9BBAA1",
                "restrictions": {
                    "restriction_names": ["Keyword"],
                    "restriction_types": ["string"],
                },
                "RestrictionsMask": "1",
            },
        ]

        ext.extend(rows)

        return generate_resp(ext)

    @staticmethod
    def discover_literals_response(request):
        """
        Generate information on literals supported by the OlaPy, including data types and values.
        :param request:
        :return:
        """
        if (request.Properties.PropertyList.Content == "SchemaData"
                and request.Properties.PropertyList.Format == "Tabular"):

            rows = discover_literals_response_rows

            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(discover_literals_xsd)
                    for resp_row in rows:
                        with xml.row:
                            for att_name, value in resp_row.items():
                                xml[att_name](value)

            return str(xml)

    def mdschema_sets_response(self, request):
        """
        Describes any sets that are currently defined in a database, including session-scoped sets.
        :param request:
        :return:
        """
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube
                and request.Properties.PropertyList.Catalog is not None):

            self.change_cube(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_sets_xsd)

            return str(xml)

    def mdschema_kpis_response(self, request):
        """
        Describes the key performance indicators (KPIs) within a database.
        :param request:
        :return:
        """
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube
                and request.Properties.PropertyList.Catalog is not None):

            self.change_cube(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_kpis_xsd)

            return str(xml)

    def dbschema_catalogs_response(self, request):
        """
        Catalogs available for a server instance
        :return:
        """
        xml = xmlwitch.Builder()
        with xml["return"]:
            with xml.root(
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                        "xmlns:xsi":
                        "http://www.w3.org/2001/XMLSchema-instance",
                    }):
                xml.write(dbschema_catalogs_xsd)
                for catalogue in self.cubes:
                    with xml.row:
                        xml.CATALOG_NAME(catalogue)

        return str(xml)

    def mdschema_cubes_response(self, request):
        """
        Describes the structure of cubes.
        :param request:
        :return:
        """
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube
                or request.Properties.PropertyList.Catalog is not None):
            self.change_cube(request.Properties.PropertyList.Catalog)
            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_cubes_xsd)
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_cube)
                        xml.CUBE_NAME(self.selected_cube)
                        xml.CUBE_TYPE("CUBE")
                        xml.LAST_SCHEMA_UPDATE("2016-07-22T10:41:38")
                        xml.LAST_DATA_UPDATE("2016-07-22T10:41:38")
                        xml.DESCRIPTION(
                            "MDX " + self.selected_cube + " results", )
                        xml.IS_DRILLTHROUGH_ENABLED("true")
                        xml.IS_LINKABLE("false")
                        xml.IS_WRITE_ENABLED("false")
                        xml.IS_SQL_ENABLED("false")
                        xml.CUBE_CAPTION(self.selected_cube)
                        xml.CUBE_SOURCE("1")

            return str(xml)

    def dbschema_tables_response(self, request):
        """
        Returns dimensions, measure groups, or schema rowsets exposed as tables.
        :param request:
        :return:
        """
        if request.Properties.PropertyList.Catalog is not None:
            self.change_cube(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(dbschema_tables_xsd)

            return str(xml)

    def mdschema_measures_response(self, request):
        """
        Returns information about the available measures.
        :param request:
        :return:
        """
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube
                and request.Properties.PropertyList.Catalog is not None):

            self.change_cube(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_measures_xsd)
                    for mes in self.executor.measures:
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_cube)
                            xml.CUBE_NAME(self.selected_cube)
                            xml.MEASURE_NAME(mes)
                            xml.MEASURE_UNIQUE_NAME("[Measures].[" + mes + "]")
                            xml.MEASURE_CAPTION(mes)
                            xml.MEASURE_AGGREGATOR("1")
                            xml.DATA_TYPE("5")
                            xml.NUMERIC_PRECISION("16")
                            xml.NUMERIC_SCALE("-1")
                            xml.MEASURE_IS_VISIBLE("true")
                            xml.MEASURE_NAME_SQL_COLUMN_NAME(mes)
                            xml.MEASURE_UNQUALIFIED_CAPTION(mes)
                            xml.MEASUREGROUP_NAME("default")

            return str(xml)

    def mdschema_dimensions_response(self, request):
        """
        Returns information about the dimensions in a given cube. Each dimension has one row.
        :param request:
        :return:
        """
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube and request.Restrictions.RestrictionList.
                CATALOG_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None):

            self.change_cube(request.Properties.PropertyList.Catalog)
            ord = 1
            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_dimensions_xsd)
                    for tables in self.executor.get_all_tables_names(
                            ignore_fact=True, ):
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_cube)
                            xml.CUBE_NAME(self.selected_cube)
                            xml.DIMENSION_NAME(tables)
                            xml.DIMENSION_UNIQUE_NAME("[" + tables + "]")
                            xml.DIMENSION_CAPTION(tables)
                            xml.DIMENSION_ORDINAL(str(ord))
                            xml.DIMENSION_TYPE("3")
                            xml.DIMENSION_CARDINALITY("23")
                            xml.DEFAULT_HIERARCHY(
                                "[" + tables + "].[" + tables + "]", )
                            xml.IS_VIRTUAL("false")
                            xml.IS_READWRITE("false")
                            xml.DIMENSION_UNIQUE_SETTINGS("1")
                            xml.DIMENSION_IS_VISIBLE("true")
                        ord += 1

                    # for measure
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_cube)
                        xml.CUBE_NAME(self.selected_cube)
                        xml.DIMENSION_NAME("Measures")
                        xml.DIMENSION_UNIQUE_NAME("[Measures]")
                        xml.DIMENSION_CAPTION("Measures")
                        xml.DIMENSION_ORDINAL(str(ord))
                        xml.DIMENSION_TYPE("2")
                        xml.DIMENSION_CARDINALITY("0")
                        xml.DEFAULT_HIERARCHY("[Measures]")
                        xml.IS_VIRTUAL("false")
                        xml.IS_READWRITE("false")
                        xml.DIMENSION_UNIQUE_SETTINGS("1")
                        xml.DIMENSION_IS_VISIBLE("true")

            return str(xml)

    def mdschema_hierarchies_response(self, request):
        """
        Describes each hierarchy within a particular dimension.
        :param request:
        :return:
        """
        # Enumeration of hierarchies in all dimensions
        restriction_list = request.Restrictions.RestrictionList

        if (restriction_list.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None):

            self.change_cube(request.Properties.PropertyList.Catalog)
            xml = xmlwitch.Builder()
            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_hierarchies_xsd)
                    if (restriction_list.HIERARCHY_VISIBILITY == 3
                            or restriction_list.CATALOG_NAME == self.
                            selected_cube):
                        for table_name, df in self.executor.tables_loaded.items():
                            if table_name == self.executor.facts:
                                continue

                            column_attribut = df.iloc[0][0]

                            with xml.row:
                                xml.CATALOG_NAME(self.selected_cube)
                                xml.CUBE_NAME(self.selected_cube)
                                xml.DIMENSION_UNIQUE_NAME(
                                    "[" + table_name + "]", )
                                xml.HIERARCHY_NAME(table_name)
                                xml.HIERARCHY_UNIQUE_NAME(
                                    "[{0}].[{0}]".format(table_name), )
                                xml.HIERARCHY_CAPTION(table_name)
                                xml.DIMENSION_TYPE("3")
                                xml.HIERARCHY_CARDINALITY("6")
                                xml.DEFAULT_MEMBER(
                                    "[{0}].[{0}].[{1}].[{2}]".format(
                                        table_name,
                                        df.columns[0],
                                        column_attribut,
                                    ), )
                                xml.STRUCTURE("0")
                                xml.IS_VIRTUAL("false")
                                xml.IS_READWRITE("false")
                                xml.DIMENSION_UNIQUE_SETTINGS("1")
                                xml.DIMENSION_IS_VISIBLE("true")
                                xml.HIERARCHY_ORDINAL("1")
                                xml.DIMENSION_IS_SHARED("true")
                                xml.HIERARCHY_IS_VISIBLE("true")
                                xml.HIERARCHY_ORIGIN("1")
                                xml.INSTANCE_SELECTION("0")

                        with xml.row:
                            xml.CATALOG_NAME(self.selected_cube)
                            xml.CUBE_NAME(self.selected_cube)
                            xml.DIMENSION_UNIQUE_NAME("[Measures]")
                            xml.HIERARCHY_NAME("Measures")
                            xml.HIERARCHY_UNIQUE_NAME("[Measures]")
                            xml.HIERARCHY_CAPTION("Measures")
                            xml.DIMENSION_TYPE("2")
                            xml.HIERARCHY_CARDINALITY("0")
                            xml.DEFAULT_MEMBER(
                                "[Measures].[{}]".format(
                                    self.executor.measures[0], ), )
                            xml.STRUCTURE("0")
                            xml.IS_VIRTUAL("false")
                            xml.IS_READWRITE("false")
                            xml.DIMENSION_UNIQUE_SETTINGS("1")
                            xml.DIMENSION_IS_VISIBLE("true")
                            xml.HIERARCHY_ORDINAL("1")
                            xml.DIMENSION_IS_SHARED("true")
                            xml.HIERARCHY_IS_VISIBLE("true")
                            xml.HIERARCHY_ORIGIN("1")
                            xml.INSTANCE_SELECTION("0")

            return str(xml)

    def mdschema_levels_response(self, request):
        """
        Returns rowset contains information about the levels available in a dimension.
        :param request:
        :return:
        """
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube
                and request.Properties.PropertyList.Catalog is not None):

            self.change_cube(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):

                    xml.write(mdschema_levels_xsd)
                    for tables in self.executor.get_all_tables_names(
                            ignore_fact=True, ):
                        l_nb = 0
                        for col in self.executor.tables_loaded[tables].columns:

                            with xml.row:
                                xml.CATALOG_NAME(self.selected_cube)
                                xml.CUBE_NAME(self.selected_cube)
                                xml.DIMENSION_UNIQUE_NAME("[" + tables + "]")
                                xml.HIERARCHY_UNIQUE_NAME(
                                    "[{0}].[{0}]".format(tables), )
                                xml.LEVEL_NAME(str(col))
                                xml.LEVEL_UNIQUE_NAME(
                                    "[{0}].[{0}].[{1}]".format(tables, col), )
                                xml.LEVEL_CAPTION(str(col))
                                xml.LEVEL_NUMBER(str(l_nb))
                                xml.LEVEL_CARDINALITY("0")
                                xml.LEVEL_TYPE("0")
                                xml.CUSTOM_ROLLUP_SETTINGS("0")
                                xml.LEVEL_UNIQUE_SETTINGS("0")
                                xml.LEVEL_IS_VISIBLE("true")
                                xml.LEVEL_DBTYPE("130")
                                xml.LEVEL_KEY_CARDINALITY("1")
                                xml.LEVEL_ORIGIN("2")
                            l_nb += 1

                    with xml.row:
                        xml.CATALOG_NAME(self.selected_cube)
                        xml.CUBE_NAME(self.selected_cube)
                        xml.DIMENSION_UNIQUE_NAME("[Measures]")
                        xml.HIERARCHY_UNIQUE_NAME("[Measures]")
                        xml.LEVEL_NAME("MeasuresLevel")
                        xml.LEVEL_UNIQUE_NAME("[Measures]")
                        xml.LEVEL_CAPTION("MeasuresLevel")
                        xml.LEVEL_NUMBER("0")
                        xml.LEVEL_CARDINALITY("0")
                        xml.LEVEL_TYPE("0")
                        xml.CUSTOM_ROLLUP_SETTINGS("0")
                        xml.LEVEL_UNIQUE_SETTINGS("0")
                        xml.LEVEL_IS_VISIBLE("true")
                        xml.LEVEL_DBTYPE("130")
                        xml.LEVEL_KEY_CARDINALITY("1")
                        xml.LEVEL_ORIGIN("2")

            return str(xml)

    def mdschema_measuregroups_response(self, request):
        """
        Describes the measure groups.
        :param request:
        :return:
        """
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube
                and request.Properties.PropertyList.Catalog is not None):

            self.change_cube(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_measuresgroups_xsd)
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_cube)
                        xml.CUBE_NAME(self.selected_cube)
                        xml.MEASUREGROUP_NAME("default")
                        xml.DESCRIPTION("-")
                        xml.IS_WRITE_ENABLED("true")
                        xml.MEASUREGROUP_CAPTION("default")

            return str(xml)

    def mdschema_measuregroup_dimensions_response(self, request):
        """
        Enumerates the dimensions of the measure groups.
        :param request:
        :return:
        """
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube
                and request.Properties.PropertyList.Catalog is not None):

            self.change_cube(request.Properties.PropertyList.Catalog)
            # rows = ""
            xml = xmlwitch.Builder()

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_measuresgroups_dimensions_xsd)
                    for tables in self.executor.get_all_tables_names(
                            ignore_fact=True, ):
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_cube)
                            xml.CUBE_NAME(self.selected_cube)
                            xml.MEASUREGROUP_NAME("default")
                            xml.MEASUREGROUP_CARDINALITY("ONE")
                            xml.DIMENSION_UNIQUE_NAME("[" + tables + "]")
                            xml.DIMENSION_CARDINALITY("MANY")
                            xml.DIMENSION_IS_VISIBLE("true")
                            xml.DIMENSION_IS_FACT_DIMENSION("false")
                            xml.DIMENSION_GRANULARITY(
                                "[{0}].[{0}]".format(tables), )

            return str(xml)

    def mdschema_properties_response(self, request):
        """
        PROPERTIES rowset contains information about the available properties for each level of the dimension
        :param request:
        :return:
        """
        xml = xmlwitch.Builder()
        if (request.Restrictions.RestrictionList.PROPERTY_TYPE == 2
                and request.Properties.PropertyList.Catalog is not None):
            properties_names = [
                "FONT_FLAGS",
                "LANGUAGE",
                "style",
                "ACTION_TYPE",
                "FONT_SIZE",
                "FORMAT_STRING",
                "className",
                "UPDATEABLE",
                "BACK_COLOR",
                "CELL_ORDINAL",
                "FONT_NAME",
                "VALUE",
                "FORMATTED_VALUE",
                "FORE_COLOR",
            ]
            properties_captions = [
                "FONT_FLAGS",
                "LANGUAGE",
                "style",
                "ACTION_TYPE",
                "FONT_SIZE",
                "FORMAT_STRING",
                "className",
                "UPDATEABLE",
                "BACK_COLOR",
                "CELL_ORDINAL",
                "FONT_NAME",
                "VALUE",
                "FORMATTED_VALUE",
                "FORE_COLOR",
            ]
            properties_datas = [
                "3",
                "19",
                "130",
                "19",
                "18",
                "130",
                "130",
                "19",
                "19",
                "19",
                "130",
                "12",
                "130",
                "19",
            ]

            self.change_cube(request.Properties.PropertyList.Catalog)

            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_properties_properties_xsd)
                    for idx, prop_name in enumerate(properties_names):
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_cube)
                            xml.PROPERTY_TYPE("2")
                            xml.PROPERTY_NAME(prop_name)
                            xml.PROPERTY_CAPTION(properties_captions[idx])
                            xml.DATA_TYPE(properties_datas[idx])

            return str(xml)

        elif request.Restrictions.RestrictionList.PROPERTY_TYPE == 1:
            with xml["return"]:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                            "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance",
                        }):
                    xml.write(mdschema_properties_properties_xsd)

            return str(xml)

    def mdschema_members_response(self, request):
        """
        Describes the members.
        :param request:
        :return:
        """
        # Enumeration of hierarchies in all dimensions
        if (request.Restrictions.RestrictionList.CUBE_NAME == self.
                selected_cube
                and request.Properties.PropertyList.Catalog is not None
                and request.Restrictions.RestrictionList.TREE_OP == 8):
            self.change_cube(request.Properties.PropertyList.Catalog)
            separed_tuple = self.executor.parser.split_tuple(
                request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME, )
            joined = ".".join(separed_tuple[:-1])
            # exple
            # separed_tuple -> [Product].[Product].[Company].[Crazy Development]
            # joined -> [Product].[Product].[Company]

            last_attribut = "".join(att for att in separed_tuple[-1] if att not in "[]").replace("&", "&amp;", )
            xml = xmlwitch.Builder()
            with xml["return"]:
                with xml.root(
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                        "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance", }):
                    xml.write(mdschema_members_xsd)
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_cube)
                        xml.CUBE_NAME(self.selected_cube)
                        xml.DIMENSION_UNIQUE_NAME(separed_tuple[0])
                        xml.HIERARCHY_UNIQUE_NAME("{0}.{0}".format(
                            separed_tuple[0], ))
                        xml.LEVEL_UNIQUE_NAME(joined)
                        xml.LEVEL_NUMBER("0")
                        xml.MEMBER_ORDINAL("0")
                        xml.MEMBER_NAME(last_attribut)
                        xml.MEMBER_UNIQUE_NAME(request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME, )
                        xml.MEMBER_TYPE("1")
                        xml.MEMBER_CAPTION(last_attribut)
                        xml.CHILDREN_CARDINALITY("1")
                        xml.PARENT_LEVEL("0")
                        xml.PARENT_COUNT("0")
                        xml.MEMBER_KEY(last_attribut)
                        xml.IS_PLACEHOLDERMEMBER("false")
                        xml.IS_DATAMEMBER("false")

            return str(xml)
