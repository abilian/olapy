# -*- encoding: utf8 -*-
"""
The main Module to manage `XMLA <https://technet.microsoft.com/fr-fr/library/ms187178(v=sql.90).aspx>`_ \
requests and responses, and managing Spyne soap server.

"""
from __future__ import absolute_import, division, print_function


# unicode_literals This is heavily discouraged with click


class XmlaProviderService():
    """
    The main class to activate SOAP services between xmla clients and olapy.
    """

    def __init__(self, discover_request_hanlder, execute_request_hanlder):
        self.discover_request_hanlder = discover_request_hanlder
        self.execute_request_hanlder = execute_request_hanlder

    def Discover(self, request):
        """The first principle function of xmla protocol.

        :param request: :class:`DiscoverRequest` object

        :return: XML Discover response as string

        """
        method_name = request.RequestType.lower() + "_response"
        method = getattr(self.discover_request_hanlder, method_name)

        if request.RequestType == "DISCOVER_DATASOURCES":
            return method()

        return method(request)

    def Execute(self, request):
        """The second principle function of xmla protocol.

        :param request: :class:`ExecuteRequest` object Execute.
        :return: XML Execute response as string
        """

        # same session_id in discover and execute
        # same executor instance as the discovery (not reloading the cube another time)
        executor = self.discover_request_hanlder.executor
        mdx_query = request.Command.Statement.encode().decode("utf8")

        # Hierarchize
        if all(key in mdx_query for key in ["WITH MEMBER", "strtomember", "[Measures].[XL_SD0]"]):
            convert2formulas = True
        else:
            convert2formulas = False

        self.execute_request_hanlder.execute_mdx_query(mdx_query, convert2formulas)

        return self.execute_request_hanlder.generate_response()
