import importlib

from pprint import pprint
from os.path import join

import pandas as pd

from olapy.core.common import DEFAULT_CUBES
from ..mdx.executor import MdxEngine
from ..mdx.executor.utils import inject_dataframes
from ..services.request_properties_models import (
    Command,
    DiscoverRequest,
    ExecuteRequest,
    Propertieslist,
    Property,
    Restriction,
    Restrictionlist,
)


class XmlaProviderLib:
    """XmlaProviderLib tu use olapy as library without running any server (no
    spyne, no wsgi...)"""

    def __init__(self, discover_request_handler, execute_request_handler):
        self.discover_request_handler = discover_request_handler
        self.execute_request_handler = execute_request_handler

    def Discover(self, request):
        """Retrieves information, such as the list of available databases,
        cubes, hierarchies or details about a specific object,from an instance
        of MdxEngine. The data retrieved with the Discover method depends on
        the values of the parameters passed to it.

        :param request: :class:`DiscoverRequest` object
        :return: XML Discover response as string
        """
        method_name = request.RequestType.lower() + "_response"
        method = getattr(self.discover_request_handler, method_name)

        if request.RequestType == "DISCOVER_DATASOURCES":
            return method()

        return method(request)

    def Execute(self, request):
        """Send xmla commands to an instance of MdxEngine.

        This includes requests involving data transfer, such as
        retrieving data on the server.

        :param request: :class:`ExecuteRequest` object Execute.
        :return: XML Execute response as string
        """

        # same session_id in discover and execute
        # same executor instance as the discovery (not reloading the cube another time)
        mdx_query = request.Command.Statement.encode().decode("utf8")

        # Hierarchize
        if all(
            key in mdx_query
            for key in ["WITH MEMBER", "strtomember", "[Measures].[XL_SD0]"]
        ):
            convert2formulas = True
        else:
            convert2formulas = False

        self.execute_request_handler.execute_mdx_query(mdx_query, convert2formulas)

        return self.execute_request_handler.generate_response()


def get_response(
    xmla_request_params,
    dataframes=None,
    output="dict",
    facts_table_name="Facts",
    mdx_engine=None,
):
    # type: (dict, dict, str, str, MdxEngine) -> dict
    """get xmla reponse.

    :param xmla_request_params: xmla request parameters
    :param dataframes: dict of pandas dataframes {df_name : df}
    :param output: xmla or dict output type
    :return: xmla response
    """
    if mdx_engine:
        executor = mdx_engine
    else:
        executor = MdxEngine(facts=facts_table_name)
    inject_dataframes(executor, dataframes, facts_table_name=facts_table_name)

    module = importlib.import_module(
        "olapy.core.services." + output + "_discover_request_handler"
    )
    discover_request_handler = getattr(module, output.title() + "DiscoverReqHandler")(
        executor
    )
    discover_request_handler.change_cube(xmla_request_params.get("cube"))

    module = importlib.import_module(
        "olapy.core.services." + output + "_execute_request_handler"
    )
    execute_request_handler = getattr(module, output.title() + "ExecuteReqHandler")(
        executor
    )

    xmla_service = XmlaProviderLib(discover_request_handler, execute_request_handler)

    properties = Propertieslist()
    properties.PropertyList = Property(**xmla_request_params.get("properties"))  # type: ignore

    if xmla_request_params.get("mdx_query"):  #  Execute request
        request = ExecuteRequest()
        request.Command = Command(  # type: ignore
            Statement=xmla_request_params.get("mdx_query")
        )

        request.Properties = properties  # type: ignore

        return xmla_service.Execute(request)

    else:  #  Discover request
        request = DiscoverRequest()  # type: ignore
        restriction = Restriction(  # type: ignore
            **xmla_request_params.get("restrictions")
        )
        request.Restrictions = Restrictionlist(  # type: ignore
            RestrictionList=restriction
        )

        request.RequestType = xmla_request_params.get("request_type")  # type: ignore
        request.Properties = properties  # type: ignore

        return xmla_service.Discover(request)


if __name__ == "__main__":
    xmla_request_params = {
        "cube": "sales",
        "request_type": "DISCOVER_PROPERTIES",
        "properties": {},
        "restrictions": {"PropertyName": "ServerName"},
        "mdx_query": None,
    }

    xmla_request_params2 = {
        "cube": "sales",
        "properties": {
            "AxisFormat": "TupleFormat",
            "Format": "Multidimensional",
            "Content": "SchemaData",
            "Catalog": "sales",
            "LocaleIdentifier": "1033",
            "Timeout": "0",
        },
        "mdx_query": """SELECT  FROM [sales] WHERE ([Measures].[Amount]) CELL PROPERTIES VALUE,
         FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS""",
    }

    # dataframes = {
    #     "Facts": pd.read_csv(
    #         "olapy-data/cubes/sales/Facts.csv", sep=";", encoding="utf8"
    #     ),
    #     "Product": pd.read_csv(
    #         "olapy-data/cubes/sales/Product.csv", sep=";", encoding="utf8"
    #     ),
    #     "Geography": pd.read_csv(
    #         "olapy-data/cubes/sales/Geography.csv", sep=";", encoding="utf8"
    #     ),
    # }
    dataframes = {
        "Facts": pd.read_csv(
            join(DEFAULT_CUBES, "sales", "Facts.csv"), sep=";", encoding="utf8"
        ),
        "Product": pd.read_csv(
            join(DEFAULT_CUBES, "sales", "Product.csv"), sep=";", encoding="utf8"
        ),
        "Geography": pd.read_csv(
            join(DEFAULT_CUBES, "sales", "Geography.csv"),
            sep=";",
            encoding="utf8",
        ),
    }

    xmla_response = get_response(xmla_request_params, dataframes, output="xmla")
    pprint(xmla_response)
    xmla_response = get_response(xmla_request_params2, dataframes, output="xmla")
    pprint(xmla_response)
