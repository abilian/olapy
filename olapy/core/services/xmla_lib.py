import importlib

import pandas as pd

from olapy.core.mdx.executor.execute import MdxEngine
from olapy.core.patch.patch_olapy import patch
from olapy.core.services.models import DiscoverRequest, Restriction, Property, Restrictionlist, Propertieslist, \
    ExecuteRequest, Command
from olapy.core.services.xmla import XmlaProviderService


class XmlaProviderLib(XmlaProviderService):
    # keep override xmla module
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
        mdx_query = request.Command.Statement.encode().decode("utf8")

        # Hierarchize
        if all(key in mdx_query for key in ["WITH MEMBER", "strtomember", "[Measures].[XL_SD0]"]):
            convert2formulas = True
        else:
            convert2formulas = False

        self.execute_request_hanlder.execute_mdx_query(mdx_query, convert2formulas)

        return self.execute_request_hanlder.generate_response()


def get_response(xmla_request_params, dataframes=None, output='dict'):
    # type: (dict, dict, str) -> dict
    """
    get xmla reponse
    :param xmla_request_params: xmla request parameters
    :param dataframes: dict of pandas dataframes {df_name : df}
    :param output: xmla or dict output type
    :return: xmla response
    """
    mdx_engine = MdxEngine()
    patch(mdx_engine, dataframes)

    module = importlib.import_module('olapy.core.services.' + output + '_discover_request_handler')
    discover_request_handler = getattr(module, output.title() + 'DiscoverReqHandler')(mdx_engine)
    discover_request_handler.change_cube(xmla_request_params.get('cube'))

    module = importlib.import_module('olapy.core.services.' + output + '_execute_request_handler')
    execute_request_handler = getattr(module, output.title() + 'ExecuteReqHandler')(mdx_engine)

    xmla_service = XmlaProviderLib(discover_request_handler,
                                   execute_request_handler)

    property = Property(**xmla_request_params.get('properties'))  # type: ignore
    properties = Propertieslist()
    properties.PropertyList = property

    if xmla_request_params.get('mdx_query'):  #  Execute request
        request = ExecuteRequest()
        request.Command = Command(Statement=xmla_request_params.get('mdx_query'))

        request.Properties = properties

        return xmla_service.Execute(request)

    else:  #  Discover request
        request = DiscoverRequest()
        restriction = Restriction(**xmla_request_params.get('restrictions'))  # type: ignore
        request.Restrictions = Restrictionlist(RestrictionList=restriction)  # type: ignore

        request.RequestType = xmla_request_params.get('request_type')  # type: ignore
        request.Properties = properties  # type: ignore

        return xmla_service.Discover(request)


if __name__ == '__main__':
    xmla_request_params = {
        'cube': 'sales',
        'request_type': 'DISCOVER_PROPERTIES',
        'properties': {
        },
        'restrictions': {
            'PropertyName': 'ServerName'
        },
        'mdx_query': None
    }

    xmla_request_params2 = {
        'cube': 'sales',
        'properties': {
            'AxisFormat': 'TupleFormat',
            'Format': 'Multidimensional',
            'Content': 'SchemaData',
            'Catalog': 'sales',
            'LocaleIdentifier': '1033',
            'Timeout': '0'
        },
        'mdx_query': """SELECT  FROM [sales] WHERE ([Measures].[Amount]) CELL PROPERTIES VALUE,
         FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""
    }

    dataframes = {'Facts': pd.read_csv("olapy-data/cubes/sales/Facts.csv", sep=';', encoding='utf8'),
                  'Product': pd.read_csv("olapy-data/cubes/sales/Product.csv", sep=';',
                                         encoding='utf8'),
                  'Geography': pd.read_csv("olapy-data/cubes/sales/Geography.csv", sep=';',
                                           encoding='utf8')
                  }

    xmla_response = get_response(xmla_request_params, dataframes)
    print(xmla_response)

    xmla_response = get_response(xmla_request_params2, dataframes)
    print(xmla_response)
