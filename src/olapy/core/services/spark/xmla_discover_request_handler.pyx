"""Managing all
`DISCOVER <https://technet.microsoft.com/fr-fr/library/ms186653(v=sql.110).aspx>`_
requests and responses."""

# import xmlwitch

from ..xmla_discover_request_handler import XmlaDiscoverReqHandler
from ..xmla_discover_xsds import mdschema_hierarchies_xsd

from olapy.stdlib.string cimport Str
from olapy.stdlib.format cimport format
from olapy.cypxml cimport cypXML, to_str


class SparkXmlaDiscoverReqHandler(XmlaDiscoverReqHandler):
    """XmlaDiscoverReqHandler handles information, such as the list of
    available databases or details about a specific object (cube, dimensions,
    hierarchies...), from an instance of MdxEngine.

    The data retrieved with the Discover method depends on the values of
    the parameters passed to it. .
    """

    def mdschema_hierarchies_response(self, request):
        """Describes each hierarchy within a particular dimension.

        :param request:
        :return:
        """
        cdef cypXML xml
        cdef Str result, column_attribut, s_table_name

        # Enumeration of hierarchies in all dimensions
        restriction_list = request.Restrictions.RestrictionList

        if (
            restriction_list.CUBE_NAME == self.selected_cube
            and request.Properties.PropertyList.Catalog is not None
        ):

            self.change_cube(request.Properties.PropertyList.Catalog)

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
            #         if (
            #             restriction_list.HIERARCHY_VISIBILITY == 3
            #             or restriction_list.CATALOG_NAME == self.selected_cube
            #         ):
            #             for table_name, df in self.executor.tables_loaded.items():
            #                 if table_name == self.executor.facts:
            #                     continue
            #
            #                 column_attribut = df.first()[0]
            #
            #                 with xml.row:
            #                     xml.CATALOG_NAME(self.selected_cube)
            #                     xml.CUBE_NAME(self.selected_cube)
            #                     xml.DIMENSION_UNIQUE_NAME("[" + table_name + "]")
            #                     xml.HIERARCHY_NAME(table_name)
            #                     xml.HIERARCHY_UNIQUE_NAME(
            #                         "[{0}].[{0}]".format(table_name)
            #                     )
            #                     xml.HIERARCHY_CAPTION(table_name)
            #                     xml.DIMENSION_TYPE("3")
            #                     xml.HIERARCHY_CARDINALITY("6")
            #                     xml.DEFAULT_MEMBER(
            #                         "[{0}].[{0}].[{1}].[{2}]".format(
            #                             table_name, df.columns[0], column_attribut
            #                         )
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
            #
            #             with xml.row:
            #                 xml.CATALOG_NAME(self.selected_cube)
            #                 xml.CUBE_NAME(self.selected_cube)
            #                 xml.DIMENSION_UNIQUE_NAME("[Measures]")
            #                 xml.HIERARCHY_NAME("Measures")
            #                 xml.HIERARCHY_UNIQUE_NAME("[Measures]")
            #                 xml.HIERARCHY_CAPTION("Measures")
            #                 xml.DIMENSION_TYPE("2")
            #                 xml.HIERARCHY_CARDINALITY("0")
            #                 xml.DEFAULT_MEMBER(
            #                     f"[Measures].[{self.executor.measures[0]}]"
            #                 )
            #                 xml.STRUCTURE("0")
            #                 xml.IS_VIRTUAL("false")
            #                 xml.IS_READWRITE("false")
            #                 xml.DIMENSION_UNIQUE_SETTINGS("1")
            #                 xml.DIMENSION_IS_VISIBLE("true")
            #                 xml.HIERARCHY_ORDINAL("1")
            #                 xml.DIMENSION_IS_SHARED("true")
            #                 xml.HIERARCHY_IS_VISIBLE("true")
            #                 xml.HIERARCHY_ORIGIN("1")
            #                 xml.INSTANCE_SELECTION("0")
            ret = xml.stag("return")
            root = ret.stag("root")
            root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
            root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
            root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
            root.append(to_str(mdschema_hierarchies_xsd))
            if (
                restriction_list.HIERARCHY_VISIBILITY == 3
                or restriction_list.CATALOG_NAME == self.selected_cube
            ):
                for table_name, df in self.executor.tables_loaded.items():
                    if table_name == self.executor.facts:
                        continue

                    column_attribut = to_str(str(df.first()[0]))
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

                    row.stag("DEFAULT_MEMBER").text(format(
                                                        "[{}].[{}].[{}].[{}]",
                                                        s_tname,
                                                        s_tname,
                                                        to_str(str(df.columns[0])),
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
