# -*- encoding: utf8 -*-

from __future__ import absolute_import, division, print_function, \
    unicode_literals

from olapy.core.services.xmla_lib import get_response


def test_dict_execute(executor):
    xmla_request_params = {
        "cube": "sales",
        "properties": {
            "AxisFormat": "TupleFormat",
            "Format": "Multidimensional",
            "Content": "SchemaData",
            "Catalog": "sales",
            "LocaleIdentifier": "1033",
            "Timeout": "0",
        },
        "mdx_query": """SELECT  FROM [sales] WHERE ([Measures].[amount]) CELL PROPERTIES VALUE,
         FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS""",
    }

    del executor.tables_loaded["time"]  # todo remove this
    xmla_response = get_response(
        xmla_request_params,
        executor.tables_loaded,
        output="dict",
        facts_table_name="facts",
        mdx_engine=executor,
    )
    assert xmla_response["cell_data"] == [1023]


def test_dict_discover(executor):
    xmla_request_params = {
        "cube": "sales",
        "request_type": "DISCOVER_PROPERTIES",
        "properties": {},
        "restrictions": {"PropertyName": "ServerName"},
        "mdx_query": None,
    }

    # del executor.tables_loaded['time']  # todo remove this
    xmla_response = get_response(
        xmla_request_params,
        executor.tables_loaded,
        output="dict",
        facts_table_name="facts",
        mdx_engine=executor,
    )
    assert xmla_response == [
        {
            "IsRequired": "false",
            "PropertyAccessType": "Read",
            "PropertyDescription": "ServerName",
            "PropertyName": "ServerName",
            "PropertyType": "string",
            "Value": "Mouadh",
        }
    ]
