"""Managing all
`DISCOVER <https://technet.microsoft.com/fr-fr/library/ms186653(v=sql.110).aspx>`_
requests and responses."""

import os
import uuid
from urllib.parse import urlparse

# import xmlwitch

#from olapy.core.mdx.executor import MdxEngine
from libcythonplus.list cimport cyplist
from olapy.core.parse import split_tuple

from olapy.core.services.structures cimport STuple, RowTuples, SchemaResponse
from olapy.core.services.xmla_discover_literals_response_rows_s cimport discover_literals_response_rows_l
from olapy.core.services.xmla_discover_schema_rowsets_response_rows_s cimport discover_schema_rowsets_response_rows_l
from olapy.core.services.xmla_discover_schema_rowsets_response_items cimport (
    MDSCHEMA_HIERARCHIES_sr, MDSCHEMA_MEASURES_sr, DBSCHEMA_TABLES_sr,
    DISCOVER_DATASOURCES_sr, DISCOVER_INSTANCES_sr, DISCOVER_KEYWORDS_sr)
from olapy.core.services.schema_response cimport discover_schema_rowsets_response_str

# from ..services.xmla_discover_request_utils import (
#     #discover_literals_response_rows,
#     discover_schema_rowsets_response_rows,
# )
from .xmla_discover_xsds import (
    dbschema_catalogs_xsd,
    dbschema_tables_xsd,
    discover_datasources_xsd,
    discover_enumerators_xsd,
    discover_keywords_xsd,
    discover_literals_xsd,
    discover_preperties_xsd,
    discover_schema_rowsets_xsd,
    mdschema_cubes_xsd,
    mdschema_dimensions_xsd,
    mdschema_functions_xsd,
    mdschema_hierarchies_xsd,
    mdschema_kpis_xsd,
    mdschema_levels_xsd,
    mdschema_measures_xsd,
    mdschema_measuresgroups_dimensions_xsd,
    mdschema_measuresgroups_xsd,
    mdschema_members_xsd,
    mdschema_properties_properties_xsd,
    mdschema_sets_xsd,
)

from olapy.stdlib.string cimport Str
from olapy.stdlib.format cimport format
from olapy.cypxml cimport cypXML, to_str

try:
    from sqlalchemy import create_engine
except ImportError:
    pass

# noinspection PyPep8Naming


class XmlaDiscoverReqHandler:
    """XmlaDiscoverReqHandler handles information, such as the list of
    available databases or details about a specific object (cube, dimensions,
    hierarchies...), from an instance of MdxEngine.

    The data retrieved with the Discover method depends on the values of
    the parameters passed to it.
    """
    def __init__(self, mdx_engine):
        # type: (MdxEngine) -> None
        """

        :param mdx_engine: mdx_engine engine instance

        """
        self.executor = mdx_engine
        if self.executor.sqla_engine:
            # save sqla uri so we can change it with new database
            self.sql_alchemy_uri = str(self.executor.sqla_engine.url)
        self.cubes = self.executor.get_cubes_names()
        self.selected_cube = None
        self.session_id = uuid.uuid1()

    def _change_db_uri(self, old_sqla_uri, new_db):
        # scheme, netloc, path, params, query, fragment = urlparse(old_sqla_uri)
        # urlunparse((scheme, netloc, new_db, params, query, fragment))
        # urlunparse -> bad result with sqlite://
        parse_uri = urlparse(old_sqla_uri)
        return parse_uri.scheme + "://" + parse_uri.netloc + "/" + new_db

    def change_cube(self, new_cube):
        """If you change the cube in any request, we have to instantiate the
        MdxEngine with the new cube.

        :param new_cube: cube name
        :return: new instance of MdxEngine with new star_schema_DataFrame and other variables
        """
        if new_cube == self.selected_cube:
            return

        if (
            self.executor.cube_config
            and new_cube == self.executor.cube_config["name"]
        ):
            facts = self.executor.cube_config["facts"]["table_name"]
        else:
            facts = "Facts"

        self.selected_cube = new_cube

        if "db" in self.executor.source_type:
            new_sql_alchemy_uri = self._change_db_uri(
                self.sql_alchemy_uri, new_cube
            )
            self.executor.sqla_engine = create_engine(new_sql_alchemy_uri)
        if self.executor.cube != new_cube:
            self.executor.load_cube(new_cube, fact_table_name=facts)

    @staticmethod
    def discover_datasources_response():
        """List the data sources available on the server.

        :return:
        """
        # Rem: This is hardcoded response
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:EX": "urn:schemas-microsoft-com:xml-analysis:exception",
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(discover_datasources_xsd)
        #         with xml.row:
        #             xml.DataSourceName("sales")
        #             xml.DataSourceDescription("sales Sample Data")
        #             xml.URL("http://127.0.0.1:8000/xmla")
        #             xml.DataSourceInfo("-")
        #             xml.ProviderName("olapy")
        #             xml.ProviderType("MDP")
        #             xml.AuthenticationMode("Unauthenticated")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:EX", "urn:schemas-microsoft-com:xml-analysis:exception")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(discover_datasources_xsd))
        row = root.stag("row")
        row.stag("DataSourceName").stext("sales")
        row.stag("DataSourceDescription").stext("sales Sample Data")
        row.stag("URL").stext("http://127.0.0.1:8000/xmla")
        row.stag("DataSourceInfo").stext("-")
        row.stag("ProviderName").stext("olapy")
        row.stag("ProviderType").stext("MDP")
        row.stag("AuthenticationMode").stext("Unauthenticated")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    @staticmethod
    def _get_properties(
        xsd,
        PropertyName,
        PropertyDescription,
        PropertyType,
        PropertyAccessType,
        IsRequired,
        Value,
    ):
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(xsd)
        #         if PropertyName:
        #
        #             with xml.row:
        #                 xml.PropertyName(PropertyName)
        #                 xml.PropertyDescription(PropertyDescription)
        #                 xml.PropertyType(PropertyType)
        #                 xml.PropertyAccessType(PropertyAccessType)
        #                 xml.IsRequired(IsRequired)
        #                 xml.Value(Value)
        #
        #         else:
        #             properties_names_n_description = [
        #                 "ServerName",
        #                 "ProviderVersion",
        #                 "MdpropMdxSubqueries",
        #                 "MdpropMdxDrillFunctions",
        #                 "MdpropMdxNamedSets",
        #             ]
        #             properties_types = ["string", "string", "int", "int", "int"]
        #             values = [
        #                 os.getenv("USERNAME", "default"),
        #                 "0.0.3  25-Nov-2016 07:20:28 GMT",
        #                 "15",
        #                 "3",
        #                 "15",
        #             ]
        #
        #             for idx, prop_desc in enumerate(properties_names_n_description):
        #                 with xml.row:
        #                     xml.PropertyName(prop_desc)
        #                     xml.PropertyDescription(prop_desc)
        #                     xml.PropertyType(properties_types[idx])
        #                     xml.PropertyAccessType("Read")
        #                     xml.IsRequired("false")
        #                     xml.Value(values[idx])
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(xsd))
        row = root.stag("row")
        if PropertyName:
            row.stag("PropertyName").text(to_str(PropertyName))
            row.stag("PropertyDescription").text(to_str(PropertyDescription))
            row.stag("PropertyType").text(to_str(PropertyType))
            row.stag("PropertyAccessType").text(to_str(PropertyAccessType))
            row.stag("IsRequired").text(to_str(IsRequired))
            row.stag("Value").text(to_str(str(Value)))
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
            for idx, prop_desc in enumerate(properties_names_n_description):
                row.stag("PropertyName").text(to_str(prop_desc))
                row.stag("PropertyDescription").text(to_str(prop_desc))
                row.stag("PropertyType").text(to_str(properties_types[idx]))
                row.stag("PropertyAccessType").stext("Read")
                row.stag("IsRequired").stext("false")
                row.stag("Value").text(to_str(values[idx]))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")


    def _get_properties_by_restrictions(self, request):
        if request.Restrictions.RestrictionList.PropertyName == "Catalog":
            if request.Properties.PropertyList.Catalog is not None:
                self.change_cube(
                    request.Properties.PropertyList.Catalog.replace("[", "").replace(
                        "]", ""
                    )
                )
                value = self.selected_cube
            else:
                value = self.cubes[0]

            return self._get_properties(
                discover_preperties_xsd,
                "Catalog",
                "Catalog",
                "string",
                "ReadWrite",
                "false",
                value,
            )

        elif request.Restrictions.RestrictionList.PropertyName == "ServerName":
            return self._get_properties(
                discover_preperties_xsd,
                "ServerName",
                "ServerName",
                "string",
                "Read",
                "false",
                "Mouadh",
            )

        elif request.Restrictions.RestrictionList.PropertyName == "ProviderVersion":
            return self._get_properties(
                discover_preperties_xsd,
                "ProviderVersion",
                "ProviderVersion",
                "string",
                "Read",
                "false",
                "0.02  08-Mar-2016 08:41:28 GMT",
            )

        elif request.Restrictions.RestrictionList.PropertyName == "MdpropMdxSubqueries":
            if request.Properties.PropertyList.Catalog is not None:
                self.change_cube(request.Properties.PropertyList.Catalog)

            return self._get_properties(
                discover_preperties_xsd,
                "MdpropMdxSubqueries",
                "MdpropMdxSubqueries",
                "int",
                "Read",
                "false",
                "15",
            )

        elif (
            request.Restrictions.RestrictionList.PropertyName
            == "MdpropMdxDrillFunctions"
        ):
            if request.Properties.PropertyList.Catalog is not None:
                self.change_cube(request.Properties.PropertyList.Catalog)

            return self._get_properties(
                discover_preperties_xsd,
                "MdpropMdxDrillFunctions",
                "MdpropMdxDrillFunctions",
                "int",
                "Read",
                "false",
                "3",
            )

        elif request.Restrictions.RestrictionList.PropertyName == "MdpropMdxNamedSets":
            return self._get_properties(
                discover_preperties_xsd,
                "MdpropMdxNamedSets",
                "MdpropMdxNamedSets",
                "int",
                "Read",
                "false",
                "15",
            )

        return self._get_properties(discover_preperties_xsd, "", "", "", "", "", "")

    def discover_properties_response(self, request):
        if request.Restrictions.RestrictionList:
            return self._get_properties_by_restrictions(request)
        return self._get_properties(discover_preperties_xsd, "", "", "", "", "", "")

    def discover_schema_rowsets_response(self, request):
        """Generate the names, restrictions, description, and other information
        for all enumeration values and any additional provider-specific
        enumeration values supported by OlaPy.

        :param request:
        :return: xmla response as string
        """
        cdef Str result
        cdef cyplist[SchemaResponse] ext

        ext = cyplist[SchemaResponse]()

        restriction_list = request.Restrictions.RestrictionList
        if restriction_list:
            if (
                restriction_list.SchemaName == "MDSCHEMA_HIERARCHIES"
                and request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)
                ext.append(MDSCHEMA_HIERARCHIES_sr)
                result = discover_schema_rowsets_response_str(ext)
                return result.bytes().decode("utf8")

            if (
                restriction_list.SchemaName == "MDSCHEMA_MEASURES"
                and request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)
                ext.append(MDSCHEMA_MEASURES_sr)
                result = discover_schema_rowsets_response_str(ext)
                return result.bytes().decode("utf8")

        ext.append(DBSCHEMA_TABLES_sr)
        ext.append(DISCOVER_DATASOURCES_sr)
        ext.append(DISCOVER_INSTANCES_sr)
        ext.append(DISCOVER_KEYWORDS_sr)
        for sr in discover_schema_rowsets_response_rows_l:
            ext.append(sr)
        result = discover_schema_rowsets_response_str(ext)
        return result.bytes().decode("utf8")

    @staticmethod
    def discover_literals_response(request):
        """Generate information on literals supported by the OlaPy, including
        data types and values.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result
        cdef RowTuples resp_row
        cdef STuple kv

        if (
            request.Properties.PropertyList.Content == "SchemaData"
            or request.Properties.PropertyList.Format == "Tabular"
        ):

            # rows = discover_literals_response_rows

            # xml = xmlwitch.Builder()
            xml = cypXML()
            xml.set_max_depth(1)
            # with xml["return"]:
            #     with xml.root(
            #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
            #         **{
            #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
            #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            #         },
            #     ):
            #         xml.write(discover_literals_xsd)
            #         for resp_row in rows:
            #             with xml.row:
            #                 for att_name, value in resp_row.items():
            #                     xml[att_name](value)
            ret = xml.stag("return")
            root = ret.stag("root")
            root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
            root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
            root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
            root.append(to_str(discover_literals_xsd))
            for resp_row in discover_literals_response_rows_l:
                row = root.stag("row")
                for kv in resp_row.row:
                    row.tag(<Str>kv.key).text(<Str>kv.value)

            # return str(xml)
            result = xml.dump()
            return result.bytes().decode("utf8")

    def mdschema_sets_response(self, request):
        """Describes any sets that are currently defined in a database,
        including session-scoped sets.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_sets_xsd)
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_sets_xsd))

        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_kpis_response(self, request):
        """Describes the key performance indicators (KPIs) within a database.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_kpis_xsd)
        #
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_kpis_xsd))

        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def dbschema_catalogs_response(self, request):
        """Catalogs available for a server instance.

        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(dbschema_catalogs_xsd)
        #         for catalogue in self.cubes:
        #             with xml.row:
        #                 xml.CATALOG_NAME(catalogue)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(dbschema_catalogs_xsd))
        for catalogue in self.cubes:
            row = root.stag("row")
            row.stag("CATALOG_NAME").text(to_str(catalogue))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_cubes_response(self, request):
        """Describes the structure of cubes.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_cubes_xsd)
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 or request.Properties.PropertyList.Catalog is not None
        #             ):
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        #
        #                 with xml.row:
        #                     xml.CATALOG_NAME(self.selected_cube)
        #                     xml.CUBE_NAME(self.selected_cube)
        #                     xml.CUBE_TYPE("CUBE")
        #                     xml.LAST_SCHEMA_UPDATE("2016-07-22T10:41:38")
        #                     xml.LAST_DATA_UPDATE("2016-07-22T10:41:38")
        #                     xml.DESCRIPTION("MDX " + self.selected_cube + " results")
        #                     xml.IS_DRILLTHROUGH_ENABLED("true")
        #                     xml.IS_LINKABLE("false")
        #                     xml.IS_WRITE_ENABLED("false")
        #                     xml.IS_SQL_ENABLED("false")
        #                     xml.CUBE_CAPTION(self.selected_cube)
        #                     xml.CUBE_SOURCE("1")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_cubes_xsd))
        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                or request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)

                row = root.stag("row")
                row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                row.stag("CUBE_TYPE").stext("CUBE")
                row.stag("LAST_SCHEMA_UPDATE").stext("2016-07-22T10:41:38")
                row.stag("LAST_DATA_UPDATE").stext("2016-07-22T10:41:38")
                row.stag("DESCRIPTION").text(to_str(
                                            "MDX " + self.selected_cube + " results"))
                row.stag("IS_DRILLTHROUGH_ENABLED").stext("true")
                row.stag("IS_LINKABLE").stext("false")
                row.stag("IS_WRITE_ENABLED").stext("false")
                row.stag("IS_SQL_ENABLED").stext("false")
                row.stag("CUBE_CAPTION").text(to_str(self.selected_cube))
                row.stag("CUBE_SOURCE").stext("1")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def dbschema_tables_response(self, request):
        """Returns dimensions, measure groups, or schema rowsets exposed as
        tables.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        if request.Properties.PropertyList.Catalog is None:
            return

        self.change_cube(request.Properties.PropertyList.Catalog)

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(dbschema_tables_xsd)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(dbschema_tables_xsd))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_measures_response(self, request):
        """Returns information about the available measures.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)

        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_measures_xsd)
        #
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        #
        #                 for mes in self.executor.measures:
        #                     with xml.row:
        #                         xml.CATALOG_NAME(self.selected_cube)
        #                         xml.CUBE_NAME(self.selected_cube)
        #                         xml.MEASURE_NAME(mes)
        #                         xml.MEASURE_UNIQUE_NAME("[Measures].[" + mes + "]")
        #                         xml.MEASURE_CAPTION(mes)
        #                         xml.MEASURE_AGGREGATOR("1")
        #                         xml.DATA_TYPE("5")
        #                         xml.NUMERIC_PRECISION("16")
        #                         xml.NUMERIC_SCALE("-1")
        #                         xml.MEASURE_IS_VISIBLE("true")
        #                         xml.MEASURE_NAME_SQL_COLUMN_NAME(mes)
        #                         xml.MEASURE_UNQUALIFIED_CAPTION(mes)
        #                         xml.MEASUREGROUP_NAME("default")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_measures_xsd))
        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)

                for mes in self.executor.measures:
                    row = root.stag("row")
                    row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                    row.stag("CUBE_NAME").text(to_str(self.selected_cube))

                    row.stag("MEASURE_NAME").text(to_str(mes))
                    row.stag("MEASURE_UNIQUE_NAME").text(to_str(
                                                        "[Measures].[" + mes + "]"))
                    row.stag("MEASURE_CAPTION").text(to_str(mes))
                    row.stag("MEASURE_AGGREGATOR").stext("1")
                    row.stag("DATA_TYPE").stext("5")
                    row.stag("NUMERIC_PRECISION").stext("16")
                    row.stag("NUMERIC_SCALE").stext("-1")
                    row.stag("MEASURE_IS_VISIBLE").stext("true")
                    row.stag("MEASURE_NAME_SQL_COLUMN_NAME").text(to_str(mes))
                    row.stag("MEASURE_UNQUALIFIED_CAPTION").text(to_str(mes))
                    row.stag("MEASUREGROUP_NAME").stext("default")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_dimensions_response(self, request):
        """Returns information about the dimensions in a given cube. Each
        dimension has one row.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(1)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_dimensions_xsd)
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Restrictions.RestrictionList.CATALOG_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        #                 ordinal = 1
        #                 for tables in self.executor.get_all_tables_names(
        #                     ignore_fact=True
        #                 ):
        #                     with xml.row:
        #                         xml.CATALOG_NAME(self.selected_cube)
        #                         xml.CUBE_NAME(self.selected_cube)
        #                         xml.DIMENSION_NAME(tables)
        #                         xml.DIMENSION_UNIQUE_NAME("[" + tables + "]")
        #                         xml.DIMENSION_CAPTION(tables)
        #                         xml.DIMENSION_ORDINAL(str(ordinal))
        #                         xml.DIMENSION_TYPE("3")
        #                         xml.DIMENSION_CARDINALITY("23")
        #                         xml.DEFAULT_HIERARCHY(
        #                             "[" + tables + "].[" + tables + "]"
        #                         )
        #                         xml.IS_VIRTUAL("false")
        #                         xml.IS_READWRITE("false")
        #                         xml.DIMENSION_UNIQUE_SETTINGS("1")
        #                         xml.DIMENSION_IS_VISIBLE("true")
        #                     ordinal += 1
        #
        #                 # for measure
        #                 with xml.row:
        #                     xml.CATALOG_NAME(self.selected_cube)
        #                     xml.CUBE_NAME(self.selected_cube)
        #                     xml.DIMENSION_NAME("Measures")
        #                     xml.DIMENSION_UNIQUE_NAME("[Measures]")
        #                     xml.DIMENSION_CAPTION("Measures")
        #                     xml.DIMENSION_ORDINAL(str(ordinal))
        #                     xml.DIMENSION_TYPE("2")
        #                     xml.DIMENSION_CARDINALITY("0")
        #                     xml.DEFAULT_HIERARCHY("[Measures]")
        #                     xml.IS_VIRTUAL("false")
        #                     xml.IS_READWRITE("false")
        #                     xml.DIMENSION_UNIQUE_SETTINGS("1")
        #                     xml.DIMENSION_IS_VISIBLE("true")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_dimensions_xsd))
        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME
                    == self.selected_cube
                and request.Restrictions.RestrictionList.CATALOG_NAME
                    == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)
                ordinal = 1
                for tables in self.executor.get_all_tables_names(ignore_fact=True):
                    row = root.stag("row")
                    row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                    row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                    row.stag("DIMENSION_NAME").text(to_str(tables))
                    row.stag("DIMENSION_UNIQUE_NAME").text(to_str("[" + tables + "]"))
                    row.stag("DIMENSION_CAPTION").text(to_str(tables))
                    row.stag("DIMENSION_ORDINAL").text(to_str(str(ordinal)))
                    row.stag("DIMENSION_TYPE").stext("3")
                    row.stag("DIMENSION_CARDINALITY").stext("23")
                    row.stag("DEFAULT_HIERARCHY").text(to_str(
                                                "[" + tables + "].[" + tables + "]"))
                    row.stag("IS_VIRTUAL").stext("false")
                    row.stag("IS_READWRITE").stext("false")
                    row.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
                    row.stag("DIMENSION_IS_VISIBLE").stext("true")
                    ordinal += 1
                # for measure
                row = root.stag("row")
                row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                row.stag("DIMENSION_NAME").stext("Measures")
                row.stag("DIMENSION_UNIQUE_NAME").stext("[Measures]")
                row.stag("DIMENSION_CAPTION").stext("Measures")
                row.stag("DIMENSION_ORDINAL").text(to_str(str(ordinal)))
                row.stag("DIMENSION_TYPE").stext("2")
                row.stag("DIMENSION_CARDINALITY").stext("0")
                row.stag("DEFAULT_HIERARCHY").stext("[Measures]")
                row.stag("IS_VIRTUAL").stext("false")
                row.stag("IS_READWRITE").stext("false")
                row.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
                row.stag("DIMENSION_IS_VISIBLE").stext("true")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_hierarchies_response(self, request):
        """Describes each hierarchy within a particular dimension.

        :param request:
        :return:
        """
        # Enumeration of hierarchies in all dimensions
        cdef cypXML xml
        cdef Str result, column_attribut, s_table_name

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(1)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_hierarchies_xsd)
        #
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        #
        #                 # if (
        #                 #     request.Restrictions.RestrictionList.HIERARCHY_VISIBILITY == 3
        #                 #     or request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_cube
        #                 # ):
        #                 for table_name, df in self.executor.tables_loaded.items():
        #                     if table_name == self.executor.facts:
        #                         continue
        #
        #                     column_attribut = df.iloc[0][0]
        #
        #                     with xml.row:
        #                         xml.CATALOG_NAME(self.selected_cube)
        #                         xml.CUBE_NAME(self.selected_cube)
        #                         xml.DIMENSION_UNIQUE_NAME("[" + table_name + "]")
        #                         xml.HIERARCHY_NAME(table_name)
        #                         xml.HIERARCHY_UNIQUE_NAME(
        #                             "[{0}].[{0}]".format(table_name)
        #                         )
        #                         xml.HIERARCHY_CAPTION(table_name)
        #                         xml.DIMENSION_TYPE("3")
        #                         xml.HIERARCHY_CARDINALITY("6")
        #                         # xml.DEFAULT_MEMBER(
        #                         #     "[{0}].[{0}].[{1}]".format(
        #                         #         table_name, column_attribut
        #                         #     )
        #                         # )
        #
        #                         # todo recheck
        #                         if (
        #                             request.Properties.PropertyList.Format
        #                             and request.Properties.PropertyList.Format.upper()
        #                             == "TABULAR"
        #                         ):
        #                             # Format found in onlyoffice and not in excel
        #                             # ALL_MEMBER causes prob with excel
        #                             xml.ALL_MEMBER(
        #                                 "[{0}].[{0}].[{1}]".format(
        #                                     table_name, column_attribut
        #                                 )
        #                             )
        #                         xml.STRUCTURE("0")
        #                         xml.IS_VIRTUAL("false")
        #                         xml.IS_READWRITE("false")
        #                         xml.DIMENSION_UNIQUE_SETTINGS("1")
        #                         xml.DIMENSION_IS_VISIBLE("true")
        #                         xml.HIERARCHY_ORDINAL("1")
        #                         xml.DIMENSION_IS_SHARED("true")
        #                         xml.HIERARCHY_IS_VISIBLE("true")
        #                         xml.HIERARCHY_ORIGIN("1")
        #                         xml.INSTANCE_SELECTION("0")
        #
        #                 with xml.row:
        #                     xml.CATALOG_NAME(self.selected_cube)
        #                     xml.CUBE_NAME(self.selected_cube)
        #                     xml.DIMENSION_UNIQUE_NAME("[Measures]")
        #                     xml.HIERARCHY_NAME("Measures")
        #                     xml.HIERARCHY_UNIQUE_NAME("[Measures]")
        #                     xml.HIERARCHY_CAPTION("Measures")
        #                     xml.DIMENSION_TYPE("2")
        #                     xml.HIERARCHY_CARDINALITY("0")
        #                     xml.DEFAULT_MEMBER(
        #                         f"[Measures].[{self.executor.measures[0]}]"
        #                     )
        #                     xml.STRUCTURE("0")
        #                     xml.IS_VIRTUAL("false")
        #                     xml.IS_READWRITE("false")
        #                     xml.DIMENSION_UNIQUE_SETTINGS("1")
        #                     xml.DIMENSION_IS_VISIBLE("true")
        #                     xml.HIERARCHY_ORDINAL("1")
        #                     xml.DIMENSION_IS_SHARED("true")
        #                     xml.HIERARCHY_IS_VISIBLE("true")
        #                     xml.HIERARCHY_ORIGIN("1")
        #                     xml.INSTANCE_SELECTION("0")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_hierarchies_xsd))
        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)
                # if (
                #     request.Restrictions.RestrictionList.HIERARCHY_VISIBILITY == 3
                #     or request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_cube
                # ):
                for table_name, df in self.executor.tables_loaded.items():
                    if table_name == self.executor.facts:
                        continue

                    column_attribut = to_str(str(df.iloc[0][0]))
                    s_tname = to_str(table_name)

                    row = root.stag("row")
                    row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                    row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                    row.stag("DIMENSION_UNIQUE_NAME").text(format("[{}]", s_tname))
                    row.stag("HIERARCHY_NAME").text(s_tname)
                    row.stag("HIERARCHY_UNIQUE_NAME").text(format(
                                                                "[{}].[{}]",
                                                                s_tname,
                                                                s_tname,
                                                            ))
                    row.stag("HIERARCHY_CAPTION").text(s_tname)
                    row.stag("DIMENSION_TYPE").stext("3")
                    row.stag("HIERARCHY_CARDINALITY").stext("6")
                    # xml.DEFAULT_MEMBER(
                    #     "[{0}].[{0}].[{1}]".format(
                    #         table_name, column_attribut
                    #     )
                    # )

                    # todo recheck
                    if (
                        request.Properties.PropertyList.Format
                        and request.Properties.PropertyList.Format.upper() == "TABULAR"
                    ):
                        # Format found in onlyoffice and not in excel
                        # ALL_MEMBER causes prob with excel
                        row.stag("ALL_MEMBER").text(format(
                                                        "[{}].[{}].[{}]",
                                                        s_tname,
                                                        s_tname,
                                                        column_attribut,
                                                    ))

                    row.stag("STRUCTURE").stext("0")
                    row.stag("IS_VIRTUAL").stext("false")
                    row.stag("IS_READWRITE").stext("false")
                    row.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
                    row.stag("DIMENSION_IS_VISIBLE").stext("true")
                    row.stag("HIERARCHY_ORDINAL").stext("1")
                    row.stag("DIMENSION_IS_SHARED").stext("true")
                    row.stag("HIERARCHY_IS_VISIBLE").stext("true")
                    row.stag("HIERARCHY_ORIGIN").stext("1")
                    row.stag("INSTANCE_SELECTION").stext("0")

                row = root.stag("row")
                row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                row.stag("DIMENSION_UNIQUE_NAME").stext("[Measures]")
                row.stag("HIERARCHY_NAME").stext("Measures")
                row.stag("HIERARCHY_UNIQUE_NAME").stext("[Measures]")
                row.stag("HIERARCHY_CAPTION").stext("Measures")
                row.stag("DIMENSION_TYPE").stext("2")
                row.stag("HIERARCHY_CARDINALITY").stext("0")
                row.stag("DEFAULT_MEMBER").text(format(
                                                "[Measures].[{}]",
                                                to_str(str(self.executor.measures[0])),
                                            ))
                row.stag("STRUCTURE").stext("0")
                row.stag("IS_VIRTUAL").stext("false")
                row.stag("IS_READWRITE").stext("false")
                row.stag("DIMENSION_UNIQUE_SETTINGS").stext("1")
                row.stag("DIMENSION_IS_VISIBLE").stext("true")
                row.stag("HIERARCHY_ORDINAL").stext("1")
                row.stag("DIMENSION_IS_SHARED").stext("true")
                row.stag("HIERARCHY_IS_VISIBLE").stext("true")
                row.stag("HIERARCHY_ORIGIN").stext("1")
                row.stag("INSTANCE_SELECTION").stext("0")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_levels_response(self, request):
        """Returns rowset contains information about the levels available in a
        dimension.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result, s_table, s_col

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(1)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_levels_xsd)
        #
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        #
        #                 for tables in self.executor.get_all_tables_names(
        #                     ignore_fact=True
        #                 ):
        #                     l_nb = 0
        #                     for col in self.executor.tables_loaded[tables].columns:
        #                         with xml.row:
        #                             xml.CATALOG_NAME(self.selected_cube)
        #                             xml.CUBE_NAME(self.selected_cube)
        #                             xml.DIMENSION_UNIQUE_NAME("[" + tables + "]")
        #                             xml.HIERARCHY_UNIQUE_NAME(
        #                                 "[{0}].[{0}]".format(tables)
        #                             )
        #                             xml.LEVEL_NAME(str(col))
        #                             xml.LEVEL_UNIQUE_NAME(
        #                                 "[{0}].[{0}].[{1}]".format(tables, col)
        #                             )
        #                             xml.LEVEL_CAPTION(str(col))
        #                             xml.LEVEL_NUMBER(str(l_nb))
        #                             xml.LEVEL_CARDINALITY("0")
        #                             xml.LEVEL_TYPE("0")
        #                             xml.CUSTOM_ROLLUP_SETTINGS("0")
        #                             xml.LEVEL_UNIQUE_SETTINGS("0")
        #                             xml.LEVEL_IS_VISIBLE("true")
        #                             xml.LEVEL_DBTYPE("130")
        #                             xml.LEVEL_KEY_CARDINALITY("1")
        #                             xml.LEVEL_ORIGIN("2")
        #                         l_nb += 1
        #
        #                 with xml.row:
        #                     xml.CATALOG_NAME(self.selected_cube)
        #                     xml.CUBE_NAME(self.selected_cube)
        #                     xml.DIMENSION_UNIQUE_NAME("[Measures]")
        #                     xml.HIERARCHY_UNIQUE_NAME("[Measures]")
        #                     xml.LEVEL_NAME("MeasuresLevel")
        #                     xml.LEVEL_UNIQUE_NAME("[Measures]")
        #                     xml.LEVEL_CAPTION("MeasuresLevel")
        #                     xml.LEVEL_NUMBER("0")
        #                     xml.LEVEL_CARDINALITY("0")
        #                     xml.LEVEL_TYPE("0")
        #                     xml.CUSTOM_ROLLUP_SETTINGS("0")
        #                     xml.LEVEL_UNIQUE_SETTINGS("0")
        #                     xml.LEVEL_IS_VISIBLE("true")
        #                     xml.LEVEL_DBTYPE("130")
        #                     xml.LEVEL_KEY_CARDINALITY("1")
        #                     xml.LEVEL_ORIGIN("2")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_levels_xsd))
        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
            ):

                self.change_cube(request.Properties.PropertyList.Catalog)

                for tables in self.executor.get_all_tables_names(ignore_fact=True):
                    l_nb = 0
                    s_table = to_str(tables)
                    for col in self.executor.tables_loaded[tables].columns:
                        s_col = to_str(str(col))
                        row = root.stag("row")
                        row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                        row.stag("CUBE_NAME").text(to_str(self.selected_cube))

                        row.stag("DIMENSION_UNIQUE_NAME").text(format(
                                                                    "[{}]",
                                                                    s_table,
                                                                ))
                        row.stag("HIERARCHY_UNIQUE_NAME").text(format(
                                                                    "[{}].[{}]",
                                                                    s_table,
                                                                    s_table,
                                                                ))
                        row.stag("LEVEL_NAME").text(to_str(str(col)))
                        row.stag("LEVEL_UNIQUE_NAME").text(format(
                                                                    "[{}].[{}].[{}]",
                                                                    s_table,
                                                                    s_table,
                                                                    s_col,
                                                                ))
                        row.stag("LEVEL_CAPTION").text(s_col)
                        row.stag("LEVEL_NUMBER").text(to_str(str(l_nb)))
                        row.stag("LEVEL_CARDINALITY").stext("0")
                        row.stag("LEVEL_TYPE").stext("0")
                        row.stag("CUSTOM_ROLLUP_SETTINGS").stext("0")
                        row.stag("LEVEL_UNIQUE_SETTINGS").stext("0")
                        row.stag("LEVEL_IS_VISIBLE").stext("true")
                        row.stag("LEVEL_DBTYPE").stext("130")
                        row.stag("LEVEL_KEY_CARDINALITY").stext("1")
                        row.stag("LEVEL_ORIGIN").stext("2")
                        l_nb += 1

                row = root.stag("row")
                row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                row.stag("DIMENSION_UNIQUE_NAME").stext("[Measures]")
                row.stag("HIERARCHY_UNIQUE_NAME").stext("[Measures]")
                row.stag("LEVEL_NAME").stext("MeasuresLevel")
                row.stag("LEVEL_UNIQUE_NAME").stext("[Measures]")
                row.stag("LEVEL_CAPTION").stext("MeasuresLevel")
                row.stag("LEVEL_NUMBER").stext("0")
                row.stag("LEVEL_CARDINALITY").stext("0")
                row.stag("LEVEL_TYPE").stext("0")
                row.stag("CUSTOM_ROLLUP_SETTINGS").stext("0")
                row.stag("LEVEL_UNIQUE_SETTINGS").stext("0")
                row.stag("LEVEL_IS_VISIBLE").stext("true")
                row.stag("LEVEL_DBTYPE").stext("130")
                row.stag("LEVEL_KEY_CARDINALITY").stext("1")
                row.stag("LEVEL_ORIGIN").stext("2")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_measuregroups_response(self, request):
        """Describes the measure groups.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_measuresgroups_xsd)
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        #
        #                 with xml.row:
        #                     xml.CATALOG_NAME(self.selected_cube)
        #                     xml.CUBE_NAME(self.selected_cube)
        #                     xml.MEASUREGROUP_NAME("default")
        #                     xml.DESCRIPTION("-")
        #                     xml.IS_WRITE_ENABLED("true")
        #                     xml.MEASUREGROUP_CAPTION("default")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_measuresgroups_xsd))
        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
            ):
                self.change_cube(request.Properties.PropertyList.Catalog)

                row = root.stag("row")
                row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                row.stag("CUBE_NAME").text(to_str(self.selected_cube))

                row.stag("MEASUREGROUP_NAME").stext("default")
                row.stag("DESCRIPTION").stext("-")
                row.stag("IS_WRITE_ENABLED").stext("true")
                row.stag("MEASUREGROUP_CAPTION").stext("default")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_measuregroup_dimensions_response(self, request):
        """Enumerates the dimensions of the measure groups.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result, s_table

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(1)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_measuresgroups_dimensions_xsd)
        #
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        #                 # rows = ""
        #
        #                 for tables in self.executor.get_all_tables_names(
        #                     ignore_fact=True
        #                 ):
        #                     with xml.row:
        #                         xml.CATALOG_NAME(self.selected_cube)
        #                         xml.CUBE_NAME(self.selected_cube)
        #                         xml.MEASUREGROUP_NAME("default")
        #                         xml.MEASUREGROUP_CARDINALITY("ONE")
        #                         xml.DIMENSION_UNIQUE_NAME("[" + tables + "]")
        #                         xml.DIMENSION_CARDINALITY("MANY")
        #                         xml.DIMENSION_IS_VISIBLE("true")
        #                         xml.DIMENSION_IS_FACT_DIMENSION("false")
        #                         xml.DIMENSION_GRANULARITY("[{0}].[{0}]".format(tables))
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_measuresgroups_dimensions_xsd))
        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
            ):

                self.change_cube(request.Properties.PropertyList.Catalog)

                for tables in self.executor.get_all_tables_names(ignore_fact=True):
                    s_table = to_str(tables)
                    row = root.stag("row")
                    row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                    row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                    row.stag("MEASUREGROUP_NAME").stext("default")
                    row.stag("MEASUREGROUP_CARDINALITY").stext("ONE")
                    row.stag("DIMENSION_UNIQUE_NAME").text(format("[{}]", s_table))
                    row.stag("DIMENSION_CARDINALITY").stext("MANY")
                    row.stag("DIMENSION_IS_VISIBLE").stext("true")
                    row.stag("DIMENSION_IS_FACT_DIMENSION").stext("false")
                    row.stag("DIMENSION_GRANULARITY").text(format(
                                                                    "[{}].[{}]",
                                                                    s_table,
                                                                    s_table,
                                                                ))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_properties_response(self, request):
        """PROPERTIES rowset contains information about the available
        properties for each level of the dimension.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(1)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_properties_properties_xsd)
        #         if request.Restrictions.RestrictionList:
        #             if (
        #                 request.Restrictions.RestrictionList.PROPERTY_TYPE == 2
        #                 and request.Properties.PropertyList.Catalog is not None
        #             ):
        #                 properties_names = [
        #                     "FONT_FLAGS",
        #                     "LANGUAGE",
        #                     "style",
        #                     "ACTION_TYPE",
        #                     "FONT_SIZE",
        #                     "FORMAT_STRING",
        #                     "className",
        #                     "UPDATEABLE",
        #                     "BACK_COLOR",
        #                     "CELL_ORDINAL",
        #                     "FONT_NAME",
        #                     "VALUE",
        #                     "FORMATTED_VALUE",
        #                     "FORE_COLOR",
        #                 ]
        #                 properties_captions = [
        #                     "FONT_FLAGS",
        #                     "LANGUAGE",
        #                     "style",
        #                     "ACTION_TYPE",
        #                     "FONT_SIZE",
        #                     "FORMAT_STRING",
        #                     "className",
        #                     "UPDATEABLE",
        #                     "BACK_COLOR",
        #                     "CELL_ORDINAL",
        #                     "FONT_NAME",
        #                     "VALUE",
        #                     "FORMATTED_VALUE",
        #                     "FORE_COLOR",
        #                 ]
        #                 properties_datas = [
        #                     "3",
        #                     "19",
        #                     "130",
        #                     "19",
        #                     "18",
        #                     "130",
        #                     "130",
        #                     "19",
        #                     "19",
        #                     "19",
        #                     "130",
        #                     "12",
        #                     "130",
        #                     "19",
        #                 ]
        #
        #                 self.change_cube(request.Properties.PropertyList.Catalog)
        #
        #                 for idx, prop_name in enumerate(properties_names):
        #                     with xml.row:
        #                         xml.CATALOG_NAME(self.selected_cube)
        #                         xml.PROPERTY_TYPE("2")
        #                         xml.PROPERTY_NAME(prop_name)
        #                         xml.PROPERTY_CAPTION(properties_captions[idx])
        #                         xml.DATA_TYPE(properties_datas[idx])
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_properties_properties_xsd))
        if request.Restrictions.RestrictionList:
            if (
                request.Restrictions.RestrictionList.PROPERTY_TYPE == 2
                and request.Properties.PropertyList.Catalog is not None
            ):
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

                for idx, prop_name in enumerate(properties_names):
                    row = root.stag("row")
                    row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                    row.stag("PROPERTY_TYPE").stext("2")
                    row.stag("PROPERTY_NAME").text(to_str(prop_name))
                    row.stag("PROPERTY_CAPTION").text(to_str(properties_captions[idx]))
                    row.stag("DATA_TYPE").text(to_str(properties_datas[idx]))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_members_response(self, request):
        """Describes the members.

        :param request:
        :return:
        """
        # Enumeration of hierarchies in all dimensions

        cdef cypXML xml
        cdef Str result, s_tuple0

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_members_xsd)
        #         if request.Restrictions.RestrictionList:
        #             self.change_cube(request.Properties.PropertyList.Catalog)
        #
        #             if request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME:
        #                 member_lvl_name = (
        #                     request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME
        #                 )
        #             else:
        #                 member_lvl_name = (
        #                     request.Restrictions.RestrictionList.LEVEL_UNIQUE_NAME
        #                 )
        #
        #             separated_tuple = self.executor.parser.split_tuple(member_lvl_name)
        #             if (
        #                 request.Restrictions.RestrictionList.CUBE_NAME
        #                 == self.selected_cube
        #                 and request.Properties.PropertyList.Catalog is not None
        #                 and request.Restrictions.RestrictionList.TREE_OP == 8
        #             ):
        #
        #                 joined = ".".join(separated_tuple[:-1])
        #                 # exple
        #                 # separed_tuple -> [Product].[Product].[Company].[Crazy Development]
        #                 # joined -> [Product].[Product].[Company]
        #
        #                 last_attribut = "".join(
        #                     att for att in separated_tuple[-1] if att not in "[]"
        #                 ).replace("&", "&amp;")
        #
        #                 with xml.row:
        #                     xml.CATALOG_NAME(self.selected_cube)
        #                     xml.CUBE_NAME(self.selected_cube)
        #                     xml.DIMENSION_UNIQUE_NAME(separated_tuple[0])
        #                     xml.HIERARCHY_UNIQUE_NAME(
        #                         "{0}.{0}".format(separated_tuple[0])
        #                     )
        #                     xml.LEVEL_UNIQUE_NAME(joined)
        #                     xml.LEVEL_NUMBER("0")
        #                     xml.MEMBER_ORDINAL("0")
        #                     xml.MEMBER_NAME(last_attribut)
        #                     xml.MEMBER_UNIQUE_NAME(member_lvl_name)
        #                     xml.MEMBER_TYPE("1")
        #                     xml.MEMBER_CAPTION(last_attribut)
        #                     xml.CHILDREN_CARDINALITY("1")
        #                     xml.PARENT_LEVEL("0")
        #                     xml.PARENT_COUNT("0")
        #                     xml.MEMBER_KEY(last_attribut)
        #                     xml.IS_PLACEHOLDERMEMBER("false")
        #                     xml.IS_DATAMEMBER("false")
        #
        #             elif member_lvl_name:
        #                 parent_level = [
        #                     "[" + tuple_att + "]" for tuple_att in separated_tuple[:-1]
        #                 ]
        #                 hierarchy_unique_name = ".".join(
        #                     ["[" + tuple_att + "]" for tuple_att in separated_tuple[:2]]
        #                 )
        #                 if len(separated_tuple) == 3:
        #                     level_unique_name = ".".join(
        #                         ["[" + tuple_att + "]" for tuple_att in separated_tuple]
        #                     )
        #                 else:
        #                     level_unique_name = ".".join(parent_level)
        #
        #                 with xml.row:
        #                     xml.CATALOG_NAME(self.selected_cube)
        #                     xml.CUBE_NAME(self.selected_cube)
        #                     xml.DIMENSION_UNIQUE_NAME("[" + separated_tuple[0] + "]")
        #                     xml.HIERARCHY_UNIQUE_NAME(hierarchy_unique_name)
        #                     xml.LEVEL_UNIQUE_NAME(level_unique_name)
        #                     xml.LEVEL_NUMBER(str(len(separated_tuple[2:])))
        #                     xml.MEMBER_ORDINAL("0")
        #                     xml.MEMBER_NAME(separated_tuple[-1])
        #                     xml.MEMBER_UNIQUE_NAME(member_lvl_name)
        #                     xml.MEMBER_TYPE("1")
        #                     xml.MEMBER_CAPTION(separated_tuple[-1])
        #                     xml.CHILDREN_CARDINALITY("1")
        #                     xml.PARENT_LEVEL("0")
        #                     xml.PARENT_COUNT("0")
        #                     xml.PARENT_UNIQUE_NAME(".".join(parent_level))
        #                     xml.MEMBER_KEY(separated_tuple[-1])
        #                     xml.IS_PLACEHOLDERMEMBER("false")
        #                     xml.IS_DATAMEMBER("false")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_members_xsd))
        if request.Restrictions.RestrictionList:
            self.change_cube(request.Properties.PropertyList.Catalog)

            if request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME:
                member_lvl_name = (
                    request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME
                )
            else:
                member_lvl_name = (
                    request.Restrictions.RestrictionList.LEVEL_UNIQUE_NAME
                )

            separated_tuple = split_tuple(member_lvl_name)
            if (
                request.Restrictions.RestrictionList.CUBE_NAME == self.selected_cube
                and request.Properties.PropertyList.Catalog is not None
                and request.Restrictions.RestrictionList.TREE_OP == 8
            ):

                joined = ".".join(separated_tuple[:-1])
                # exple
                # separed_tuple -> [Product].[Product].[Company].[Crazy Development]
                # joined -> [Product].[Product].[Company]

                last_attribut = "".join(
                    att for att in separated_tuple[-1] if att not in "[]"
                ).replace("&", "&amp;")

                s_tuple0 = to_str(str(separated_tuple[0]))

                row = root.stag("row")
                row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                row.stag("DIMENSION_UNIQUE_NAME").text(s_tuple0)
                row.stag("HIERARCHY_UNIQUE_NAME").text(format(
                                                            "{}.{}",
                                                            s_tuple0,
                                                            s_tuple0,
                                                        ))
                row.stag("LEVEL_UNIQUE_NAME").text(to_str(joined))
                row.stag("LEVEL_NUMBER").stext("0")
                row.stag("MEMBER_ORDINAL").stext("0")
                row.stag("MEMBER_NAME").text(to_str(last_attribut))
                row.stag("MEMBER_UNIQUE_NAME").text(to_str(str(member_lvl_name)))
                row.stag("MEMBER_TYPE").stext("1")
                row.stag("MEMBER_CAPTION").text(to_str(last_attribut))
                row.stag("CHILDREN_CARDINALITY").stext("1")
                row.stag("PARENT_LEVEL").stext("0")
                row.stag("PARENT_COUNT").stext("0")
                row.stag("MEMBER_KEY").text(to_str(last_attribut))
                row.stag("IS_PLACEHOLDERMEMBER").stext("false")
                row.stag("IS_DATAMEMBER").stext("false")

            elif member_lvl_name:
                parent_level = [
                    "[" + tuple_att + "]" for tuple_att in separated_tuple[:-1]
                ]
                hierarchy_unique_name = ".".join(
                    ["[" + tuple_att + "]" for tuple_att in separated_tuple[:2]]
                )
                if len(separated_tuple) == 3:
                    level_unique_name = ".".join(
                        ["[" + tuple_att + "]" for tuple_att in separated_tuple]
                    )
                else:
                    level_unique_name = ".".join(parent_level)

                row = root.stag("row")
                row.stag("CATALOG_NAME").text(to_str(self.selected_cube))
                row.stag("CUBE_NAME").text(to_str(self.selected_cube))
                row.stag("DIMENSION_UNIQUE_NAME").text(to_str(
                                                    "[" + separated_tuple[0] + "]"))
                row.stag("HIERARCHY_UNIQUE_NAME").text(to_str(hierarchy_unique_name))
                row.stag("LEVEL_UNIQUE_NAME").text(to_str(level_unique_name))
                row.stag("LEVEL_NUMBER").text(to_str(str(len(separated_tuple[2:]))))
                row.stag("MEMBER_ORDINAL").stext("0")
                row.stag("MEMBER_NAME").text(to_str(str(separated_tuple[-1])))
                row.stag("MEMBER_UNIQUE_NAME").text(to_str(member_lvl_name))
                row.stag("MEMBER_TYPE").stext("1")
                row.stag("MEMBER_CAPTION").text(to_str(str(separated_tuple[-1])))
                row.stag("CHILDREN_CARDINALITY").stext("1")
                row.stag("PARENT_LEVEL").stext("0")
                row.stag("PARENT_COUNT").stext("0")
                row.stag("PARENT_UNIQUE_NAME").text(to_str(".".join(parent_level)))
                row.stag("MEMBER_KEY").text(to_str(separated_tuple[-1]))
                row.stag("IS_PLACEHOLDERMEMBER").stext("false")
                row.stag("IS_DATAMEMBER").stext("false")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def discover_instances_response(self, request):
        """todo.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(discover_schema_rowsets_xsd)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(discover_schema_rowsets_xsd))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def dmschema_mining_models_response(self, request):
        """todo.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(discover_schema_rowsets_xsd)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(discover_schema_rowsets_xsd))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_actions_response(self, request):
        """todo.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(discover_schema_rowsets_xsd)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(discover_schema_rowsets_xsd))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_functions_response(self, request):
        """todo.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(mdschema_functions_xsd)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(mdschema_functions_xsd))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def mdschema_input_datasources_response(self, request):
        """todo.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(discover_schema_rowsets_xsd)
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(discover_schema_rowsets_xsd))

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def discover_enumerators_response(self, request):
        """todo."""

        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(discover_enumerators_xsd)
        #
        #         with xml.row:
        #             xml.EnumName("ProviderType")
        #             xml.ElementName("TDP")
        #             xml.EnumType("string")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(discover_enumerators_xsd))
        row = root.stag("row")
        row.stag("EnumName").stext("ProviderType")
        row.stag("ElementName").stext("TDP")
        row.stag("EnumType").stext("string")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")

    def discover_keywords_response(self, request):
        """todo."""

        cdef cypXML xml
        cdef Str result

        # xml = xmlwitch.Builder()
        xml = cypXML()
        xml.set_max_depth(0)
        # with xml["return"]:
        #     with xml.root(
        #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
        #         **{
        #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
        #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        #         },
        #     ):
        #         xml.write(discover_keywords_xsd)
        #         with xml.row:
        #             xml.Keyword("aggregate")
        #             xml.Keyword("ancestors")
        ret = xml.stag("return")
        root = ret.stag("root")
        root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
        root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
        root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
        root.append(to_str(discover_keywords_xsd))
        row = root.stag("row")
        row.stag("Keyword").stext("aggregate")
        row.stag("Keyword").stext("ancestors")

        # return str(xml)
        result = xml.dump()
        return result.bytes().decode("utf8")
