# -*- encoding: utf8 -*-

from __future__ import absolute_import, division, print_function

import uuid
import xmlwitch
import HTMLParser

import os

from ..mdx.executor.execute import MdxEngine
from .xmla_discover_xsds import (
    dbschema_catalogs_xsd, dbschema_tables_xsd, discover_datasources_xsd,
    discover_literals_xsd, discover_preperties_xsd,
    discover_schema_rowsets_xsd, mdschema_cubes_xsd, mdschema_dimensions_xsd,
    mdschema_hierarchies_xsd, mdschema_kpis_xsd, mdschema_levels_xsd,
    mdschema_measures_xsd, mdschema_measuresgroups_dimensions_xsd,
    mdschema_measuresgroups_xsd, mdschema_members_xsd,
    mdschema_properties_properties_xsd, mdschema_sets_xsd)


# TODO clean
class XmlaDiscoverTools():
    """XmlaDiscoverTools for generating xmla discover responses."""

    def __init__(self):
        # right now the catalogue_name and cube name are the same
        # todo bug double catalogue to fix
        self.catalogues = MdxEngine.get_cubes_names()
        self.selected_catalogue = self.catalogues[0]
        self.executer = MdxEngine(self.selected_catalogue)
        self.star_schema_dataframe = self.executer.load_star_schema_dataframe[[
            col for col in self.executer.load_star_schema_dataframe.columns
            if col[-3:] != "_id"
        ]]
        self.session_id = uuid.uuid1()

    def change_catalogue(self, new_catalogue):
        """
        If you change the catalogue (cube) in any request, we have to instantiate the MdxEngine with the new catalogue.

        :param new_catalogue: catalogue name
        :return: new instance of MdxEngine with new star_schema_DataFrame and other variables
        """
        if self.selected_catalogue != new_catalogue:
            self.selected_catalogue = new_catalogue
            self.executer = MdxEngine(new_catalogue)
            self.star_schema_dataframe = self.executer.load_star_schema_dataframe[
                [
                    col
                    for col in self.executer.load_star_schema_dataframe.columns
                    if col[-3:] != "_id"
                ]]

    @staticmethod
    def discover_datasources_response():

        xml = xmlwitch.Builder()
        with xml['return']:
            with xml.root(
                    discover_datasources_xsd,
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        'xmlns:EX':
                        'urn:schemas-microsoft-com:xml-analysis:exception',
                        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi':
                        'http://www.w3.org/2001/XMLSchema-instance'
                    }):
                with xml.row:
                    xml.DataSourceName('sales')
                    xml.DataSourceDescription('sales Sample Data')
                    xml.URL('http://127.0.0.1:8000/xmla')
                    xml.DataSourceInfo('-')
                    xml.ProviderName('olapy')
                    xml.ProviderType('MDP')
                    xml.AuthenticationMode('Unauthenticated')

        return xml

    def discover_properties_response(self, request):
        def get_props(xsd, PropertyName, PropertyDescription, PropertyType,
                      PropertyAccessType, IsRequired, Value):

            xml = xmlwitch.Builder()

            if PropertyName is not '':
                with xml['return']:
                    with xml.root(
                            xsd,
                            xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                            **{
                                'xmlns:xsd':
                                'http://www.w3.org/2001/XMLSchema',
                                'xmlns:xsi':
                                'http://www.w3.org/2001/XMLSchema-instance'
                            }):

                        with xml.row:
                            xml.PropertyName(PropertyName)
                            xml.PropertyDescription(PropertyDescription)
                            xml.PropertyType(PropertyType)
                            xml.PropertyAccessType(PropertyAccessType)
                            xml.IsRequired(IsRequired)
                            xml.Value(Value)

            else:
                properties_names_n_description = [
                    'ServerName', 'ProviderVersion', 'MdpropMdxSubqueries',
                    'MdpropMdxDrillFunctions', 'MdpropMdxNamedSets'
                ]
                properties_types = ['string', 'string', 'int', 'int', 'int']
                values = [
                    os.getenv('USERNAME', 'default'),
                    '0.0.3  25-Nov-2016 07:20:28 GMT', '15', '3', '15'
                ]

                with xml['return']:
                    with xml.root(
                            xsd,
                            xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                            **{
                                'xmlns:xsd':
                                'http://www.w3.org/2001/XMLSchema',
                                'xmlns:xsi':
                                'http://www.w3.org/2001/XMLSchema-instance'
                            }):
                        for idx, prop_desc in enumerate(
                                properties_names_n_description):
                            with xml.row:
                                xml.PropertyName(prop_desc)
                                xml.PropertyDescription(prop_desc)
                                xml.PropertyType(properties_types[idx])
                                xml.PropertyAccessType('Read')
                                xml.IsRequired('false')
                                xml.Value(values[idx])

            # escape gt; lt; (from xsd)
            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml

        if request.Restrictions.RestrictionList.PropertyName == 'Catalog':
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                value = self.selected_catalogue
            else:
                value = "olapy Unspecified Catalog"
            return get_props(discover_preperties_xsd, 'Catalog', 'Catalog',
                             'string', 'ReadWrite', 'false', value)

        elif request.Restrictions.RestrictionList.PropertyName == 'ServerName':
            return get_props(discover_preperties_xsd, 'ServerName',
                             'ServerName', 'string', 'Read', 'false', 'Mouadh')

        elif request.Restrictions.RestrictionList.PropertyName == 'ProviderVersion':
            return get_props(discover_preperties_xsd, 'ProviderVersion',
                             'ProviderVersion', 'string', 'Read', 'false',
                             '0.02  08-Mar-2016 08:41:28 GMT')

        elif request.Restrictions.RestrictionList.PropertyName == 'MdpropMdxSubqueries':
            if 'Unspecified' in request.Properties.PropertyList.Catalog:
                return get_props(discover_preperties_xsd,
                                 'MdpropMdxSubqueries', 'MdpropMdxSubqueries',
                                 'int', 'Read', 'false', '15')

            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return get_props(discover_preperties_xsd,
                                 'MdpropMdxSubqueries', 'MdpropMdxSubqueries',
                                 'int', 'Read', 'false', '15')

        elif request.Restrictions.RestrictionList.PropertyName == 'MdpropMdxDrillFunctions':
            if 'Unspecified' in request.Properties.PropertyList.Catalog:
                return get_props(
                    discover_preperties_xsd, 'MdpropMdxDrillFunctions',
                    'MdpropMdxDrillFunctions', 'int', 'Read', 'false', '3')

            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return get_props(
                    discover_preperties_xsd, 'MdpropMdxDrillFunctions',
                    'MdpropMdxDrillFunctions', 'int', 'Read', 'false', '3')

        elif request.Restrictions.RestrictionList.PropertyName == 'MdpropMdxNamedSets':
            if 'Unspecified' in request.Properties.PropertyList.Catalog:
                return get_props(discover_preperties_xsd, 'MdpropMdxNamedSets',
                                 'MdpropMdxNamedSets', 'int', 'Read', 'false',
                                 '15')

            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return get_props(discover_preperties_xsd, 'MdpropMdxNamedSets',
                                 'MdpropMdxNamedSets', 'int', 'Read', 'false',
                                 '15')

        return get_props(discover_preperties_xsd, '', '', '', '', '', '')

    def discover_schema_rowsets_response(self, request):

        rows = [
            {
                'SchemaName': 'DBSCHEMA_CATALOGS',
                'SchemaGuid': 'C8B52211-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': ['CATALOG_NAME'],
                    'restriction_types': ['string']
                },
                'RestrictionsMask': '1'
            },
            {
                'SchemaName': 'DISCOVER_LITERALS',
                'SchemaGuid': 'C3EF5ECB-0A07-4665-A140-B075722DBDC2',
                'restrictions': {
                    'restriction_names': ['LiteralName'],
                    'restriction_types': ['string']
                },
                'RestrictionsMask': '1'
            },
            {
                'SchemaName': 'DISCOVER_PROPERTIES',
                'SchemaGuid': '4B40ADFB-8B09-4758-97BB-636E8AE97BCF',
                'restrictions': {
                    'restriction_names': ['PropertyName'],
                    'restriction_types': ['string']
                },
                'RestrictionsMask': '1'
            },
            {
                'SchemaName': 'DISCOVER_SCHEMA_ROWSETS',
                'SchemaGuid': 'EEA0302B-7922-4992-8991-0E605D0E5593',
                'restrictions': {
                    'restriction_names': ['SchemaName'],
                    'restriction_types': ['string']
                },
                'RestrictionsMask': '1'
            },
            {
                'SchemaName': 'DMSCHEMA_MINING_MODELS',
                'SchemaGuid': '3ADD8A77-D8B9-11D2-8D2A-00E029154FDE',
                'restrictions': {
                    'restriction_names': [
                        'MODEL_CATALOG', 'MODEL_SCHEMA', 'MODEL_NAME',
                        'MODEL_TYPE', 'SERVICE_NAME', 'SERVICE_TYPE_ID',
                        'MINING_STRUCTURE'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'string',
                        'unsignedInt', 'string'
                    ]
                },
                'RestrictionsMask': '127'
            },
            {
                'SchemaName': 'MDSCHEMA_ACTIONS',
                'SchemaGuid': 'A07CCD08-8148-11D0-87BB-00C04FC33942',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'ACTION_NAME', 'ACTION_TYPE', 'COORDINATE',
                        'COORDINATE_TYPE', 'INVOCATION', 'CUBE_SOURCE'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'int',
                        'string', 'int', 'int', 'unsignedShort'
                    ]
                },
                'RestrictionsMask': '511'
            },
            {
                'SchemaName': 'MDSCHEMA_CUBES',
                'SchemaGuid': 'C8B522D8-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'CUBE_SOURCE', 'BASE_CUBE_NAME'
                    ],
                    'restriction_types':
                    ['string', 'string', 'string', 'unsignedShort', 'string']
                },
                'RestrictionsMask': '31'
            },
            {
                'SchemaName': 'MDSCHEMA_DIMENSIONS',
                'SchemaGuid': 'C8B522D9-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'DIMENSION_NAME', 'DIMENSION_UNIQUE_NAME',
                        'CUBE_SOURCE', 'DIMENSION_VISIBILITY'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'string',
                        'unsignedShort', 'unsignedShort'
                    ]
                },
                'RestrictionsMask': '127'
            },
            {
                'SchemaName': 'MDSCHEMA_FUNCTIONS',
                'SchemaGuid': 'A07CCD07-8148-11D0-87BB-00C04FC33942',
                'restrictions': {
                    'restriction_names': [
                        'LIBRARY_NAME', 'INTERFACE_NAME', 'FUNCTION_NAME',
                        'ORIGIN'
                    ],
                    'restriction_types': ['string', 'string', 'string', 'int']
                },
                'RestrictionsMask': '15'
            },
            {
                'SchemaName': 'MDSCHEMA_HIERARCHIES',
                'SchemaGuid': 'C8B522DA-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'DIMENSION_UNIQUE_NAME', 'HIERARCHY_NAME',
                        'HIERARCHY_UNIQUE_NAME', 'HIERARCHY_ORIGIN',
                        'CUBE_SOURCE', 'HIERARCHY_VISIBILITY'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'string',
                        'string', 'unsignedShort', 'unsignedShort',
                        'unsignedShort'
                    ]
                },
                'RestrictionsMask': '511'
            },
            {
                'SchemaName': 'MDSCHEMA_INPUT_DATASOURCES',
                'SchemaGuid': 'A07CCD32-8148-11D0-87BB-00C04FC33942',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'DATASOURCE_NAME',
                        'DATASOURCE_TYPE'
                    ],
                    'restriction_types':
                    ['string', 'string', 'string', 'string']
                },
                'RestrictionsMask': '15'
            },
            {
                'SchemaName': 'MDSCHEMA_KPIS',
                'SchemaGuid': '2AE44109-ED3D-4842-B16F-B694D1CB0E3F',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME', 'KPI_NAME',
                        'CUBE_SOURCE'
                    ],
                    'restriction_types':
                    ['string', 'string', 'string', 'string', 'unsignedShort']
                },
                'RestrictionsMask': '31'
            },
            {
                'SchemaName': 'MDSCHEMA_LEVELS',
                'SchemaGuid': 'C8B522DB-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'DIMENSION_UNIQUE_NAME', 'HIERARCHY_UNIQUE_NAME',
                        'LEVEL_NAME', 'LEVEL_UNIQUE_NAME', 'LEVEL_ORIGIN',
                        'CUBE_SOURCE', 'LEVEL_VISIBILITY'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'string',
                        'string', 'string', 'unsignedShort', 'unsignedShort',
                        'unsignedShort'
                    ]
                },
                'RestrictionsMask': '1023'
            },
            {
                'SchemaName': 'MDSCHEMA_MEASUREGROUPS',
                'SchemaGuid': 'E1625EBF-FA96-42FD-BEA6-DB90ADAFD96B',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'MEASUREGROUP_NAME'
                    ],
                    'restriction_types':
                    ['string', 'string', 'string', 'string']
                },
                'RestrictionsMask': '15'
            },
            {
                'SchemaName': 'MDSCHEMA_MEASUREGROUP_DIMENSIONS',
                'SchemaGuid': 'A07CCD33-8148-11D0-87BB-00C04FC33942',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'MEASUREGROUP_NAME', 'DIMENSION_UNIQUE_NAME',
                        'DIMENSION_VISIBILITY'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'string',
                        'unsignedShort'
                    ]
                },
                'RestrictionsMask': '63'
            },
            {
                'SchemaName': 'MDSCHEMA_MEASURES',
                'SchemaGuid': 'C8B522DC-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'MEASURE_NAME', 'MEASURE_UNIQUE_NAME',
                        'MEASUREGROUP_NAME', 'CUBE_SOURCE',
                        'MEASURE_VISIBILITY'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'string',
                        'string', 'unsignedShort', 'unsignedShort'
                    ]
                },
                'RestrictionsMask': '255'
            },
            {
                'SchemaName': 'MDSCHEMA_MEMBERS',
                'SchemaGuid': 'C8B522DE-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'DIMENSION_UNIQUE_NAME', 'HIERARCHY_UNIQUE_NAME',
                        'LEVEL_UNIQUE_NAME', 'LEVEL_NUMBER', 'MEMBER_NAME',
                        'MEMBER_UNIQUE_NAME', 'MEMBER_CAPTION', 'MEMBER_TYPE',
                        'TREE_OP', 'CUBE_SOURCE'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'string',
                        'string', 'unsignedInt', 'string', 'string', 'string',
                        'int', 'int', 'unsignedShort'
                    ]
                },
                'RestrictionsMask': '8191'
            },
            {
                'SchemaName': 'MDSCHEMA_PROPERTIES',
                'SchemaGuid': 'C8B522DD-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                        'DIMENSION_UNIQUE_NAME', 'HIERARCHY_UNIQUE_NAME',
                        'LEVEL_UNIQUE_NAME', 'MEMBER_UNIQUE_NAME',
                        'PROPERTY_NAME', 'PROPERTY_TYPE',
                        'PROPERTY_CONTENT_TYPE', 'PROPERTY_ORIGIN',
                        'CUBE_SOURCE', 'PROPERTY_VISIBILITY'
                    ],
                    'restriction_types': [
                        'string', 'string', 'string', 'string', 'string',
                        'string', 'string', 'string', 'string', 'string',
                        'unsignedShort', 'unsignedShort', 'unsignedShort'
                    ]
                },
                'RestrictionsMask': '8191'
            },
            {
                'SchemaName': 'MDSCHEMA_SETS',
                'SchemaGuid': 'A07CCD0B-8148-11D0-87BB-00C04FC33942',
                'restrictions': {
                    'restriction_names': [
                        'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME', 'SET_NAME',
                        'SCOPE'
                    ],
                    'restriction_types':
                    ['string', 'string', 'string', 'string', 'int']
                },
                'RestrictionsMask': '31'
            },
        ]

        def generate_resp(rows):
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        discover_schema_rowsets_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):

                    for resp_row in rows:
                        with xml.row:
                            xml.SchemaName(resp_row['SchemaName'])
                            xml.SchemaGuid(resp_row['SchemaGuid'])
                            for idx, restriction in enumerate(resp_row[
                                    'restrictions']['restriction_names']):
                                with xml.Restrictions:
                                    xml.Name(restriction)
                                    xml.Type(resp_row['restrictions'][
                                        'restriction_types'][idx])

                            xml.RestrictionsMask(resp_row['RestrictionsMask'])

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml

        if request.Restrictions.RestrictionList.SchemaName == "MDSCHEMA_HIERARCHIES" and \
                        request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)

            restriction_names = [
                'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME',
                'DIMENSION_UNIQUE_NAME', 'HIERARCHY_NAME',
                'HIERARCHY_UNIQUE_NAME', 'HIERARCHY_ORIGIN', 'CUBE_SOURCE',
                'HIERARCHY_VISIBILITY'
            ]
            restriction_types = [
                'string', 'string', 'string', 'string', 'string', 'string',
                'unsignedShort', 'unsignedShort', 'unsignedShort'
            ]

            rows = [{
                'SchemaName': 'MDSCHEMA_HIERARCHIES',
                'SchemaGuid': 'C8B522DA-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': restriction_names,
                    'restriction_types': restriction_types
                },
                'RestrictionsMask': '511'
            }]

            return generate_resp(rows)


        if request.Restrictions.RestrictionList.SchemaName == 'MDSCHEMA_MEASURES' and \
                        request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)

            restriction_names = [
                'CATALOG_NAME', 'SCHEMA_NAME', 'CUBE_NAME', 'MEASURE_NAME',
                'MEASURE_UNIQUE_NAME', 'MEASUREGROUP_NAME', 'CUBE_SOURCE',
                'MEASURE_VISIBILITY'
            ]
            restriction_types = [
                'string', 'string', 'string', 'string', 'string', 'string',
                'unsignedShort', 'unsignedShort'
            ]

            rows = [{
                'SchemaName': 'MDSCHEMA_MEASURES',
                'SchemaGuid': 'C8B522DC-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': restriction_names,
                    'restriction_types': restriction_types
                },
                'RestrictionsMask': '255'
            }]

            return generate_resp(rows)

        # TODO delete
        if request.Properties.PropertyList.Catalog is not None:
            return generate_resp(rows)

        ext = [{
            'SchemaName': 'DBSCHEMA_TABLES',
            'SchemaGuid': 'C8B52229-5CF3-11CE-ADE5-00AA0044773D',
            'restrictions': {
                'restriction_names': [
                    'TABLE_CATALOG', 'TABLE_SCHEMA', 'TABLE_NAME',
                    'TABLE_TYPE', 'TABLE_OLAP_TYPE'
                ],
                'restriction_types':
                ['string', 'string', 'string', 'string', 'string']
            },
            'RestrictionsMask': '31'
        }, {
            'SchemaName': 'DISCOVER_DATASOURCES',
            'SchemaGuid': '06C03D41-F66D-49F3-B1B8-987F7AF4CF18',
            'restrictions': {
                'restriction_names': [
                    'DataSourceName', 'URL', 'ProviderName', 'ProviderType',
                    'AuthenticationMode'
                ],
                'restriction_types':
                ['string', 'string', 'string', 'string', 'string']
            },
            'RestrictionsMask': '31'
        }, {
            'SchemaName': 'DISCOVER_INSTANCES',
            'SchemaGuid': '20518699-2474-4C15-9885-0E947EC7A7E3',
            'restrictions': {
                'restriction_names': ['INSTANCE_NAME'],
                'restriction_types': ['string']
            },
            'RestrictionsMask': '1'
        }, {
            'SchemaName': 'DISCOVER_KEYWORDS',
            'SchemaGuid': '1426C443-4CDD-4A40-8F45-572FAB9BBAA1',
            'restrictions': {
                'restriction_names': ['Keyword'],
                'restriction_types': ['string']
            },
            'RestrictionsMask': '1'
        }]

        ext.extend(rows)

        return generate_resp(ext)

    @staticmethod
    def discover_literals_response(request):
        if request.Properties.PropertyList.Content == 'SchemaData' \
                and request.Properties.PropertyList.Format == 'Tabular':

            rows = [
                {
                    'LiteralName': 'DBLITERAL_CATALOG_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '24',
                    'LiteralNameEnumValue': '2'
                },
                {
                    'LiteralName': 'DBLITERAL_CATALOG_SEPARATOR',
                    'LiteralValue': '.',
                    'LiteralMaxLength': '0',
                    'LiteralNameEnumValue': '3'
                },
                {
                    'LiteralName': 'DBLITERAL_COLUMN_ALIAS',
                    'LiteralInvalidChars': "'&quot;[]",
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '5'
                },
                {
                    'LiteralName': 'DBLITERAL_COLUMN_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '6'
                },
                {
                    'LiteralName': 'DBLITERAL_CORRELATION_NAME',
                    'LiteralInvalidChars': "'&quot;[]",
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '7'
                },
                {
                    'LiteralName': 'DBLITERAL_CUBE_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '21'
                },
                {
                    'LiteralName': 'DBLITERAL_DIMENSION_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '22'
                },
                {
                    'LiteralName': 'DBLITERAL_LEVEL_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '24'
                },
                {
                    'LiteralName': 'DBLITERAL_MEMBER_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '25'
                },
                {
                    'LiteralName': 'DBLITERAL_PROCEDURE_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '14'
                },
                {
                    'LiteralName': 'DBLITERAL_PROPERTY_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '26'
                },
                {
                    'LiteralName': 'DBLITERAL_QUOTE_PREFIX',
                    'LiteralValue': '[',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '15'
                },
                {
                    'LiteralName': 'DBLITERAL_QUOTE_SUFFIX',
                    'LiteralValue': ']',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '28'
                },
                {
                    'LiteralName': 'DBLITERAL_TABLE_NAME',
                    'LiteralInvalidChars': '.',
                    'LiteralInvalidStartingChars': '0123456789',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '17'
                },
                {
                    'LiteralName': 'DBLITERAL_TEXT_COMMAND',
                    'LiteralMaxLength': '-1',
                    'LiteralNameEnumValue': '18'
                },
                {
                    'LiteralName': 'DBLITERAL_USER_NAME',
                    'LiteralMaxLength': '0',
                    'LiteralNameEnumValue': '19'
                },
            ]

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        discover_literals_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):

                    for resp_row in rows:
                        with xml.row:
                            for att_name, value in resp_row.items():
                                xml[att_name](value)

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml

    def discover_mdschema_sets_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue \
                and request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml['return']:
                xml.root(
                    mdschema_sets_xsd,
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi':
                        'http://www.w3.org/2001/XMLSchema-instance'
                    })

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml

    def discover_mdschema_kpis_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue \
                and request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml['return']:
                xml.root(
                    mdschema_kpis_xsd,
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi':
                        'http://www.w3.org/2001/XMLSchema-instance'
                    })

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml

    def discover_dbschema_catalogs_response(self, request):
        xml = xmlwitch.Builder()
        with xml['return']:
            with xml.root(
                    dbschema_catalogs_xsd,
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi':
                        'http://www.w3.org/2001/XMLSchema-instance'
                    }):

                for catalogue in self.catalogues:
                    with xml.row:
                        xml.CATALOG_NAME(catalogue)

        html_parser = HTMLParser.HTMLParser()
        xml = html_parser.unescape(str(xml))

        return xml

    def discover_mdschema_cubes_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue \
                or request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        mdschema_cubes_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_catalogue)
                        xml.CUBE_NAME(self.selected_catalogue)
                        xml.CUBE_TYPE('CUBE')
                        xml.LAST_SCHEMA_UPDATE('2016-07-22T10:41:38')
                        xml.LAST_DATA_UPDATE('2016-07-22T10:41:38')
                        xml.DESCRIPTION('MDX ' + self.selected_catalogue +
                                        'results')
                        xml.IS_DRILLTHROUGH_ENABLED('true')
                        xml.IS_LINKABLE('false')
                        xml.IS_WRITE_ENABLED('false')
                        xml.IS_SQL_ENABLED('false')
                        xml.CUBE_CAPTION(self.selected_catalogue)
                        xml.CUBE_SOURCE('1')

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml

    def discover_dbschema_tables_response(self, request):
        if request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml['return']:
                xml.root(
                    dbschema_tables_xsd,
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi':
                        'http://www.w3.org/2001/XMLSchema-instance'
                    })

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml

    def discover_mdschema_measures__response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                        request.Restrictions.RestrictionList.MEASURE_VISIBILITY == 3 and\
                        request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        mdschema_measures_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):
                    for mes in self.executer.measures:
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_catalogue)
                            xml.CUBE_NAME(self.selected_catalogue)
                            xml.MEASURE_NAME(mes)
                            xml.MEASURE_UNIQUE_NAME('[Measures].[' + mes + ']')
                            xml.MEASURE_CAPTION(mes)
                            xml.MEASURE_AGGREGATOR('1')
                            xml.DATA_TYPE('5')
                            xml.NUMERIC_PRECISION('16')
                            xml.NUMERIC_SCALE('-1')
                            xml.MEASURE_IS_VISIBLE('true')
                            xml.MEASURE_NAME_SQL_COLUMN_NAME(mes)
                            xml.MEASURE_UNQUALIFIED_CAPTION(mes)
                            xml.MEASUREGROUP_NAME('default')

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml

    def discover_mdschema_dimensions_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and\
                        request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue and \
                        request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            ord = 1
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        mdschema_dimensions_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):

                    for tables in self.executer.get_all_tables_names(
                            ignore_fact=True):
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_catalogue)
                            xml.CUBE_NAME(self.selected_catalogue)
                            xml.DIMENSION_NAME(tables)
                            xml.DIMENSION_UNIQUE_NAME('[' + tables + ']')
                            xml.DIMENSION_CAPTION(tables)
                            xml.DIMENSION_ORDINAL(str(ord))
                            xml.DIMENSION_TYPE('3')
                            xml.DIMENSION_CARDINALITY('23')
                            xml.DEFAULT_HIERARCHY('[' + tables + '].[' + tables
                                                  + ']')
                            xml.IS_VIRTUAL('false')
                            xml.IS_READWRITE('false')
                            xml.DIMENSION_UNIQUE_SETTINGS('1')
                            xml.DIMENSION_IS_VISIBLE('true')
                        ord += 1

                    # for measure
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_catalogue)
                        xml.CUBE_NAME(self.selected_catalogue)
                        xml.DIMENSION_NAME('Measures')
                        xml.DIMENSION_UNIQUE_NAME('[Measures]')
                        xml.DIMENSION_CAPTION('Measures')
                        xml.DIMENSION_ORDINAL(str(ord))
                        xml.DIMENSION_TYPE('2')
                        xml.DIMENSION_CARDINALITY('0')
                        xml.DEFAULT_HIERARCHY('[Measures]')
                        xml.IS_VIRTUAL('false')
                        xml.IS_READWRITE('false')
                        xml.DIMENSION_UNIQUE_SETTINGS('1')
                        xml.DIMENSION_IS_VISIBLE('true')

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml

    def discover_mdschema_hierarchies_response(self, request):

        # Enumeration of hierarchies in all dimensions
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                        request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            if request.Restrictions.RestrictionList.HIERARCHY_VISIBILITY == 3 or \
                            request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue:

                xml = xmlwitch.Builder()

                with xml['return']:
                    with xml.root(
                            mdschema_hierarchies_xsd,
                            xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                            **{
                                'xmlns:xsd':
                                'http://www.w3.org/2001/XMLSchema',
                                'xmlns:xsi':
                                'http://www.w3.org/2001/XMLSchema-instance'
                            }):

                        for table_name, df in self.executer.tables_loaded.items(
                        ):
                            if table_name == self.executer.facts:
                                continue

                            # french caracteres
                            # TODO encode dataframe
                            if type(df.iloc[0][0]) == unicode:
                                column_attribut = df.iloc[0][0].encode(
                                    'utf-8', 'replace')
                            else:
                                column_attribut = df.iloc[0][0]

                            with xml.row:
                                xml.CATALOG_NAME(self.selected_catalogue)
                                xml.CUBE_NAME(self.selected_catalogue)
                                xml.DIMENSION_UNIQUE_NAME('[' + table_name +
                                                          ']')
                                xml.HIERARCHY_NAME(table_name)
                                xml.HIERARCHY_UNIQUE_NAME(
                                    '[{0}].[{0}]'.format(table_name))
                                xml.HIERARCHY_CAPTION(table_name)
                                xml.DIMENSION_TYPE('3')
                                xml.HIERARCHY_CARDINALITY('6')
                                xml.DEFAULT_MEMBER(
                                    '[{0}].[{0}].[{1}].[{2}]'.format(
                                        table_name, df.columns[
                                            0], column_attribut))
                                xml.STRUCTURE('0')
                                xml.IS_VIRTUAL('false')
                                xml.IS_READWRITE('false')
                                xml.DIMENSION_UNIQUE_SETTINGS('1')
                                xml.DIMENSION_IS_VISIBLE('true')
                                xml.HIERARCHY_ORDINAL('1')
                                xml.DIMENSION_IS_SHARED('true')
                                xml.HIERARCHY_IS_VISIBLE('true')
                                xml.HIERARCHY_ORIGIN('1')
                                xml.INSTANCE_SELECTION('0')

                        with xml.row:
                            xml.CATALOG_NAME(self.selected_catalogue)
                            xml.CUBE_NAME(self.selected_catalogue)
                            xml.DIMENSION_UNIQUE_NAME('[Measures]')
                            xml.HIERARCHY_NAME('Measures')
                            xml.HIERARCHY_UNIQUE_NAME('[Measures]')
                            xml.HIERARCHY_CAPTION('Measures')
                            xml.DIMENSION_TYPE('2')
                            xml.HIERARCHY_CARDINALITY('0')
                            xml.DEFAULT_MEMBER('[Measures].[{0}]'.format(
                                self.executer.measures[0]))
                            xml.STRUCTURE('0')
                            xml.IS_VIRTUAL('false')
                            xml.IS_READWRITE('false')
                            xml.DIMENSION_UNIQUE_SETTINGS('1')
                            xml.DIMENSION_IS_VISIBLE('true')
                            xml.HIERARCHY_ORDINAL('1')
                            xml.DIMENSION_IS_SHARED('true')
                            xml.HIERARCHY_IS_VISIBLE('true')
                            xml.HIERARCHY_ORIGIN('1')
                            xml.INSTANCE_SELECTION('0')

                html_parser = HTMLParser.HTMLParser()
                xml = html_parser.unescape(str(xml))
                return xml

    def discover_mdschema_levels__response(self, request):
        # TODO fix levels in the same table (with xml file maybe) !!!!!!!!!
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                        request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue and \
                        request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        mdschema_levels_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):

                    for tables in self.executer.get_all_tables_names(
                            ignore_fact=True):
                        l_nb = 0
                        for col in self.executer.tables_loaded[tables].columns:

                            with xml.row:
                                xml.CATALOG_NAME(self.selected_catalogue)
                                xml.CUBE_NAME(self.selected_catalogue)
                                xml.DIMENSION_UNIQUE_NAME('[' + tables + ']')
                                xml.HIERARCHY_UNIQUE_NAME(
                                    '[{0}].[{0}]'.format(tables))
                                xml.LEVEL_NAME(str(col))
                                xml.LEVEL_UNIQUE_NAME(
                                    '[{0}].[{0}].[{1}]'.format(tables, col))
                                xml.LEVEL_CAPTION(str(col))
                                xml.LEVEL_NUMBER(str(l_nb))
                                xml.LEVEL_CARDINALITY('0')
                                xml.LEVEL_TYPE('0')
                                xml.CUSTOM_ROLLUP_SETTINGS('0')
                                xml.LEVEL_UNIQUE_SETTINGS('0')
                                xml.LEVEL_IS_VISIBLE('true')
                                xml.LEVEL_DBTYPE('130')
                                xml.LEVEL_KEY_CARDINALITY('1')
                                xml.LEVEL_ORIGIN('2')
                            l_nb += 1

                    with xml.row:
                        xml.CATALOG_NAME(self.selected_catalogue)
                        xml.CUBE_NAME(self.selected_catalogue)
                        xml.DIMENSION_UNIQUE_NAME('[Measures]')
                        xml.HIERARCHY_UNIQUE_NAME('[Measures]')
                        xml.LEVEL_NAME('MeasuresLevel')
                        xml.LEVEL_UNIQUE_NAME('[Measures]')
                        xml.LEVEL_CAPTION('MeasuresLevel')
                        xml.LEVEL_NUMBER('0')
                        xml.LEVEL_CARDINALITY('0')
                        xml.LEVEL_TYPE('0')
                        xml.CUSTOM_ROLLUP_SETTINGS('0')
                        xml.LEVEL_UNIQUE_SETTINGS('0')
                        xml.LEVEL_IS_VISIBLE('true')
                        xml.LEVEL_DBTYPE('130')
                        xml.LEVEL_KEY_CARDINALITY('1')
                        xml.LEVEL_ORIGIN('2')

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml

    def discover_mdschema_measuresgroups_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        mdschema_measuresgroups_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_catalogue)
                        xml.CUBE_NAME(self.selected_catalogue)
                        xml.MEASUREGROUP_NAME('CUBE_NAME')
                        xml.DESCRIPTION('-')
                        xml.IS_WRITE_ENABLED('true')
                        xml.MEASUREGROUP_CAPTION('default')

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml

    def discover_mdschema_measuresgroups_dimensions_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            # rows = ""
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        mdschema_measuresgroups_dimensions_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):
                    for tables in self.executer.get_all_tables_names(
                            ignore_fact=True):
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_catalogue)
                            xml.CUBE_NAME(self.selected_catalogue)
                            xml.MEASUREGROUP_NAME('default')
                            xml.MEASUREGROUP_CARDINALITY('ONE')
                            xml.DIMENSION_UNIQUE_NAME('[' + tables + ']')
                            xml.DIMENSION_CARDINALITY('MANY')
                            xml.DIMENSION_IS_VISIBLE('true')
                            xml.DIMENSION_IS_FACT_DIMENSION('false')
                            xml.DIMENSION_GRANULARITY(
                                '[{0}].[{0}]'.format(tables))

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml

    def discover_mdschema_properties_response(self, request):
        xml = xmlwitch.Builder()
        if request.Restrictions.RestrictionList.PROPERTY_TYPE == 2 and \
            request.Properties.PropertyList.Catalog is not None:
            properties_names = [
                'FONT_FLAGS', 'LANGUAGE', 'style', 'ACTION_TYPE', 'FONT_SIZE',
                'FORMAT_STRING', 'className', 'UPDATEABLE', 'BACK_COLOR',
                'CELL_ORDINAL', 'FONT_NAME', 'VALUE', 'FORMATTED_VALUE',
                'FORE_COLOR'
            ]
            properties_captions = [
                'FONT_FLAGS', 'LANGUAGE', 'style', 'ACTION_TYPE', 'FONT_SIZE',
                'FORMAT_STRING', 'className', 'UPDATEABLE', 'BACK_COLOR',
                'CELL_ORDINAL', 'FONT_NAME', 'VALUE', 'FORMATTED_VALUE',
                'FORE_COLOR'
            ]
            properties_datas = [
                '3', '19', '130', '19', '18', '130', '130', '19', '19', '19',
                '130', '12', '130', '19'
            ]

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            with xml['return']:
                with xml.root(
                        mdschema_properties_properties_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):
                    for idx, prop_name in enumerate(properties_names):
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_catalogue)
                            xml.PROPERTY_TYPE('2')
                            xml.PROPERTY_NAME(prop_name)
                            xml.PROPERTY_CAPTION(properties_captions[idx])
                            xml.DATA_TYPE(properties_datas[idx])

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml

        elif request.Restrictions.RestrictionList.PROPERTY_TYPE == 1:
            with xml['return']:
                xml.root(
                    mdschema_properties_properties_xsd,
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi':
                        'http://www.w3.org/2001/XMLSchema-instance'
                    })

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml

    def discover_mdschema_members_response(self, request):
        # Enumeration of hierarchies in all dimensions
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None and \
                request.Restrictions.RestrictionList.TREE_OP == 8:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            separed_tuple = request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME.split(
                ".")
            joined = ".".join(separed_tuple[:-1])
            # exple
            # separed_tuple -> [Product].[Product].[Company].[Crazy Development]
            # joined -> [Product].[Product].[Company]

            last_attribut = ''.join(att for att in separed_tuple[-1]
                                    if att not in '[]').replace('&', '&amp;')
            xml = xmlwitch.Builder()
            with xml['return']:
                with xml.root(
                        mdschema_members_xsd,
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance'
                        }):
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_catalogue)
                        xml.CUBE_NAME(self.selected_catalogue)
                        xml.DIMENSION_UNIQUE_NAME(separed_tuple[0])
                        xml.HIERARCHY_UNIQUE_NAME(
                            '{0}.{0}'.format(separed_tuple[0]))
                        xml.LEVEL_UNIQUE_NAME(joined)
                        xml.LEVEL_NUMBER('0')
                        xml.MEMBER_ORDINAL('0')
                        xml.MEMBER_NAME(last_attribut)
                        xml.MEMBER_UNIQUE_NAME(
                            request.Restrictions.RestrictionList.
                            MEMBER_UNIQUE_NAME)
                        xml.MEMBER_TYPE('1')
                        xml.MEMBER_CAPTION(last_attribut)
                        xml.CHILDREN_CARDINALITY('1')
                        xml.PARENT_LEVEL('0')
                        xml.PARENT_COUNT('0')
                        xml.MEMBER_KEY(last_attribut)
                        xml.IS_PLACEHOLDERMEMBER('false')
                        xml.IS_DATAMEMBER('false')

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))
            return xml
