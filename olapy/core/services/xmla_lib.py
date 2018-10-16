import pandas as pd

from olapy.core.mdx.executor.execute import MdxEngine
from olapy.core.patch.patch_olapy import patch
from olapy.core.services.models import DiscoverRequest, Restriction, Property, Restrictionlist, Propertieslist, \
    ExecuteRequest, Command
from olapy.core.services.xmla import XmlaProviderService
from olapy.core.services.xmla_discover_tools import XmlaTools


def run_xmla(xmla_request_params, dataframes=None):
    mdx_engine = MdxEngine()
    patch(mdx_engine, dataframes)
    xmla_tools = XmlaTools(mdx_engine)  # xmla tools prepares xmla responses
    xmla_tools.change_catalogue(xmla_request_params.get('cube'))

    xmla_service = XmlaProviderService(xmla_tools)  # xmla provider return xmla responses

    property = Property(**xmla_request_params.get('properties'))
    properties = Propertieslist()
    properties.PropertyList = property

    if xmla_request_params.get('mdx_query'):  #  Execute request
        request = ExecuteRequest()
        request.Command = Command(Statement=xmla_request_params.get('mdx_query'))

        request.Properties = properties

        return xmla_service.Execute(request)

    else:  #  Discover request
        request = DiscoverRequest()
        restriction = Restriction(**xmla_request_params.get('restrictions'))
        request.Restrictions = Restrictionlist(RestrictionList=restriction)

        request.RequestType = xmla_request_params.get('request_type')
        request.Properties = properties

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

    xmla_response = run_xmla(xmla_request_params, dataframes)
    print(xmla_response)

    xmla_response = run_xmla(xmla_request_params2, dataframes)
    print(xmla_response)
