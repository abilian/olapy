# -*- encoding: utf8 -*-
"""
The main Module to manage `XMLA <https://technet.microsoft.com/fr-fr/library/ms187178(v=sql.90).aspx>`_ \
requests and responses, and managing Spyne soap server.

"""
from __future__ import absolute_import, division, print_function
# unicode_literals This is heavily discouraged with click

import os
from datetime import datetime
from os.path import expanduser
import xmlwitch
from .xmla_execute_tools import XmlaExecuteTools
from .xmla_execute_xsds import execute_xsd


class XmlaProviderService():
    """
    The main class to activate SOAP services between xmla clients and olapy.
    """

    def __init__(self, xmla_tools):
        self.xmla_tools = xmla_tools

    def Discover(self, request):
        """The first principle function of xmla protocol.

        :param request: :class:`DiscoverRequest` object

        :return: XML Discover response as string

        """

        method_name = request.RequestType.lower() + "_response"
        method = getattr(self.xmla_tools, method_name)

        if request.RequestType == "DISCOVER_DATASOURCES":
            return method()

        return method(request)

    def Execute(self, request):
        """The second principle function of xmla protocol.

        :param request: :class:`ExecuteRequest` object Execute.
        :return: XML Execute response as string
        """

        mdx_query = request.Command.Statement.encode().decode("utf8")
        if mdx_query == "":
            # check if command contains a query
            xml = xmlwitch.Builder()
            with xml["return"]:
                xml.root(xmlns="urn:schemas-microsoft-com:xml-analysis:empty")

            return str(xml)

        else:
            self.xmla_tools.change_catalogue(request.Properties.PropertyList.Catalog,
                                             )
            xml = xmlwitch.Builder()
            executor = self.xmla_tools.executor

            # Hierarchize
            if all(key in mdx_query
                   for key in
                   ["WITH MEMBER", "strtomember", "[Measures].[XL_SD0]"]):
                convert2formulas = True
            else:
                convert2formulas = False

            xmla_tools = XmlaExecuteTools(
                executor,
                mdx_query,
                convert2formulas,
            )

            with xml["return"]:
                with xml.root(
                    xmlns="urn:schemas-microsoft-com:xml-analysis:mddataset",
                    **{
                        "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
                        "xmlns:xsi":
                            "http://www.w3.org/2001/XMLSchema-instance", }):
                    xml.write(execute_xsd)
                    with xml.OlapInfo:
                        with xml.CubeInfo:
                            with xml.Cube:
                                xml.CubeName("Sales")
                                xml.LastDataUpdate(
                                    datetime.now().strftime(
                                        "%Y-%m-%dT%H:%M:%S", ),
                                    xmlns="http://schemas.microsoft.com/analysisservices/2003/engine",
                                )
                                xml.LastSchemaUpdate(
                                    datetime.now().strftime(
                                        "%Y-%m-%dT%H:%M:%S", ),
                                    xmlns="http://schemas.microsoft.com/analysisservices/2003/engine",
                                )
                        xml.write(xmla_tools.generate_cell_info())
                        with xml.AxesInfo:
                            xml.write(xmla_tools.generate_axes_info())
                            xml.write(xmla_tools.generate_axes_info_slicer())

                    with xml.Axes:
                        xml.write(xmla_tools.generate_xs0())
                        xml.write(xmla_tools.generate_slicer_axis())

                    with xml.CellData:
                        xml.write(xmla_tools.generate_cell_data())
            return str(xml)


home_directory = expanduser("~")
conf_file = os.path.join(home_directory, "olapy-data", "logs", "xmla.log")
