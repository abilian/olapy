"""Managing all `DISCOVER <https://technet.microsoft.com/fr-
fr/library/ms186653(v=sql.110).aspx>`_ requests and responses."""
# -*- encoding: utf8 -*-

from __future__ import absolute_import, division, print_function, \
    unicode_literals

import xmlwitch

from ..xmla_discover_request_handler import XmlaDiscoverReqHandler
from ..xmla_discover_xsds import mdschema_hierarchies_xsd


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
        # Enumeration of hierarchies in all dimensions
        restriction_list = request.Restrictions.RestrictionList

        if (
            restriction_list.CUBE_NAME == self.selected_cube
            and request.Properties.PropertyList.Catalog is not None
        ):

            self.change_cube(request.Properties.PropertyList.Catalog)
            xml = xmlwitch.Builder()
            with xml["return"]:
                with xml.root(
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                    }
                ):
                    xml.write(mdschema_hierarchies_xsd)
                    if (
                        restriction_list.HIERARCHY_VISIBILITY == 3
                        or restriction_list.CATALOG_NAME == self.selected_cube
                    ):
                        for table_name, df in self.executor.tables_loaded.items():
                            if table_name == self.executor.facts:
                                continue

                            column_attribut = df.first()[0]

                            with xml.row:
                                xml.CATALOG_NAME(self.selected_cube)
                                xml.CUBE_NAME(self.selected_cube)
                                xml.DIMENSION_UNIQUE_NAME("[" + table_name + "]")
                                xml.HIERARCHY_NAME(table_name)
                                xml.HIERARCHY_UNIQUE_NAME(
                                    "[{0}].[{0}]".format(table_name)
                                )
                                xml.HIERARCHY_CAPTION(table_name)
                                xml.DIMENSION_TYPE("3")
                                xml.HIERARCHY_CARDINALITY("6")
                                xml.DEFAULT_MEMBER(
                                    "[{0}].[{0}].[{1}].[{2}]".format(
                                        table_name, df.columns[0], column_attribut
                                    )
                                )
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
                                "[Measures].[{}]".format(self.executor.measures[0])
                            )
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
