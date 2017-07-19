# -*- encoding: utf8 -*-

from __future__ import absolute_import, division, print_function

import uuid

import xmlwitch
import HTMLParser


from lxml import etree
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
            with xml.root(discover_datasources_xsd, xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                          **{'xmlns:EX': 'urn:schemas-microsoft-com:xml-analysis:exception',
                             'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                             'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'}
                          ):
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
                    with xml.root(xsd, xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                                  **{'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                                     'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'}):

                        with xml.row:
                            xml.PropertyName(PropertyName)
                            xml.PropertyDescription(PropertyDescription)
                            xml.PropertyType(PropertyType)
                            xml.PropertyAccessType(PropertyAccessType)
                            xml.IsRequired(IsRequired)
                            xml.Value(Value)

            else:
                properties_names_n_description = ['ServerName',
                                  'ProviderVersion',
                                  'MdpropMdxSubqueries',
                                  'MdpropMdxDrillFunctions',
                                  'MdpropMdxNamedSets']
                properties_types = ['string',
                                  'string',
                                  'int',
                                  'int',
                                  'int']
                values = [os.getenv('USERNAME', 'default'),
                          '0.0.3  25-Nov-2016 07:20:28 GMT',
                          '15',
                          '3',
                          '15']

                with xml['return']:
                    with xml.root(xsd, xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                                  **{'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                                     'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'}):
                        for idx,prop_desc in enumerate(properties_names_n_description):
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

        rows = [{'SchemaName': 'DBSCHEMA_CATALOGS',
                 'SchemaGuid': 'C8B52211-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {'restriction_names': ['CATALOG_NAME'],
                                  'restriction_types': ['string']},
                 'RestrictionsMask': '1'
                 },
                {'SchemaName': 'DISCOVER_LITERALS',
                 'SchemaGuid': 'C3EF5ECB-0A07-4665-A140-B075722DBDC2',
                 'restrictions': {
                     'restriction_names': ['LiteralName'],
                     'restriction_types': ['string']},
                 'RestrictionsMask': '1'
                 },
                {'SchemaName': 'DISCOVER_PROPERTIES',
                 'SchemaGuid': '4B40ADFB-8B09-4758-97BB-636E8AE97BCF',
                 'restrictions': {
                     'restriction_names': ['PropertyName'],
                     'restriction_types': ['string']},
                 'RestrictionsMask': '1'
                 },
                {'SchemaName': 'DISCOVER_SCHEMA_ROWSETS',
                 'SchemaGuid': 'EEA0302B-7922-4992-8991-0E605D0E5593',
                 'restrictions': {
                     'restriction_names': ['SchemaName'],
                     'restriction_types': ['string']},
                 'RestrictionsMask': '1'
                 },
                {'SchemaName': 'DMSCHEMA_MINING_MODELS',
                 'SchemaGuid': '3ADD8A77-D8B9-11D2-8D2A-00E029154FDE',
                 'restrictions': {
                     'restriction_names': ['MODEL_CATALOG',
                                           'MODEL_SCHEMA',
                                           'MODEL_NAME',
                                           'MODEL_TYPE',
                                           'SERVICE_NAME',
                                           'SERVICE_TYPE_ID',
                                           'MINING_STRUCTURE'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedInt',
                                           'string']},
                 'RestrictionsMask': '127'
                 },
                {'SchemaName': 'MDSCHEMA_ACTIONS',
                 'SchemaGuid': 'A07CCD08-8148-11D0-87BB-00C04FC33942',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'ACTION_NAME',
                                           'ACTION_TYPE',
                                           'COORDINATE',
                                           'COORDINATE_TYPE',
                                           'INVOCATION',
                                           'CUBE_SOURCE'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'int',
                                           'string',
                                           'int',
                                           'int',
                                           'unsignedShort']},
                 'RestrictionsMask': '511'
                 },
                {'SchemaName': 'MDSCHEMA_CUBES',
                 'SchemaGuid': 'C8B522D8-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'CUBE_SOURCE',
                                           'BASE_CUBE_NAME'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'unsignedShort',
                                           'string']},
                 'RestrictionsMask': '31'
                 },
                {'SchemaName': 'MDSCHEMA_DIMENSIONS',
                 'SchemaGuid': 'C8B522D9-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'DIMENSION_NAME',
                                           'DIMENSION_UNIQUE_NAME',
                                           'CUBE_SOURCE',
                                           'DIMENSION_VISIBILITY'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedShort',
                                           'unsignedShort']},
                 'RestrictionsMask': '127'
                 },
                {'SchemaName': 'MDSCHEMA_FUNCTIONS',
                 'SchemaGuid': 'A07CCD07-8148-11D0-87BB-00C04FC33942',
                 'restrictions': {
                     'restriction_names': ['LIBRARY_NAME',
                                           'INTERFACE_NAME',
                                           'FUNCTION_NAME',
                                           'ORIGIN'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'int']},
                 'RestrictionsMask': '15'
                 },
                {'SchemaName': 'MDSCHEMA_HIERARCHIES',
                 'SchemaGuid': 'C8B522DA-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'DIMENSION_UNIQUE_NAME',
                                           'HIERARCHY_NAME',
                                           'HIERARCHY_UNIQUE_NAME',
                                           'HIERARCHY_ORIGIN',
                                           'CUBE_SOURCE',
                                           'HIERARCHY_VISIBILITY'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedShort',
                                           'unsignedShort',
                                           'unsignedShort']},
                 'RestrictionsMask': '511'
                 },
                {'SchemaName': 'MDSCHEMA_INPUT_DATASOURCES',
                 'SchemaGuid': 'A07CCD32-8148-11D0-87BB-00C04FC33942',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'DATASOURCE_NAME',
                                           'DATASOURCE_TYPE'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string']},
                 'RestrictionsMask': '15'
                 },
                {'SchemaName': 'MDSCHEMA_KPIS',
                 'SchemaGuid': '2AE44109-ED3D-4842-B16F-B694D1CB0E3F',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'KPI_NAME',
                                           'CUBE_SOURCE'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedShort']},
                 'RestrictionsMask': '31'
                 },
                {'SchemaName': 'MDSCHEMA_LEVELS',
                 'SchemaGuid': 'C8B522DB-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'DIMENSION_UNIQUE_NAME',
                                           'HIERARCHY_UNIQUE_NAME',
                                           'LEVEL_NAME',
                                           'LEVEL_UNIQUE_NAME',
                                           'LEVEL_ORIGIN',
                                           'CUBE_SOURCE',
                                           'LEVEL_VISIBILITY'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedShort',
                                           'unsignedShort',
                                           'unsignedShort']},
                 'RestrictionsMask': '1023'
                 },
                {'SchemaName': 'MDSCHEMA_MEASUREGROUPS',
                 'SchemaGuid': 'E1625EBF-FA96-42FD-BEA6-DB90ADAFD96B',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'MEASUREGROUP_NAME'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string']},
                 'RestrictionsMask': '15'
                 },
                {'SchemaName': 'MDSCHEMA_MEASUREGROUP_DIMENSIONS',
                 'SchemaGuid': 'A07CCD33-8148-11D0-87BB-00C04FC33942',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'MEASUREGROUP_NAME',
                                           'DIMENSION_UNIQUE_NAME',
                                           'DIMENSION_VISIBILITY'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedShort']},
                 'RestrictionsMask': '63'
                 },
                {'SchemaName': 'MDSCHEMA_MEASURES',
                 'SchemaGuid': 'C8B522DC-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'MEASURE_NAME',
                                           'MEASURE_UNIQUE_NAME',
                                           'MEASUREGROUP_NAME',
                                           'CUBE_SOURCE',
                                           'MEASURE_VISIBILITY'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedShort',
                                           'unsignedShort']},
                 'RestrictionsMask': '255'
                 },
                {'SchemaName': 'MDSCHEMA_MEMBERS',
                 'SchemaGuid': 'C8B522DE-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'DIMENSION_UNIQUE_NAME',
                                           'HIERARCHY_UNIQUE_NAME',
                                           'LEVEL_UNIQUE_NAME',
                                           'LEVEL_NUMBER',
                                           'MEMBER_NAME',
                                           'MEMBER_UNIQUE_NAME',
                                           'MEMBER_CAPTION',
                                           'MEMBER_TYPE',
                                           'TREE_OP',
                                           'CUBE_SOURCE'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedInt',
                                           'string',
                                           'string',
                                           'string',
                                           'int',
                                           'int',
                                           'unsignedShort']},
                 'RestrictionsMask': '8191'
                 },
                {'SchemaName': 'MDSCHEMA_PROPERTIES',
                 'SchemaGuid': 'C8B522DD-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'DIMENSION_UNIQUE_NAME',
                                           'HIERARCHY_UNIQUE_NAME',
                                           'LEVEL_UNIQUE_NAME',
                                           'MEMBER_UNIQUE_NAME',
                                           'PROPERTY_NAME',
                                           'PROPERTY_TYPE',
                                           'PROPERTY_CONTENT_TYPE',
                                           'PROPERTY_ORIGIN',
                                           'CUBE_SOURCE',
                                           'PROPERTY_VISIBILITY'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'string',
                                           'unsignedShort',
                                           'unsignedShort',
                                           'unsignedShort']},
                 'RestrictionsMask': '8191'
                 },
                {'SchemaName': 'MDSCHEMA_SETS',
                 'SchemaGuid': 'A07CCD0B-8148-11D0-87BB-00C04FC33942',
                 'restrictions': {
                     'restriction_names': ['CATALOG_NAME',
                                           'SCHEMA_NAME',
                                           'CUBE_NAME',
                                           'SET_NAME',
                                           'SCOPE'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'int']},
                 'RestrictionsMask': '31'
                 },

                ]

        def generate_resp(rows):
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(discover_schema_rowsets_xsd, xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                              **{'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                                 'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'}):

                    for resp_row in rows:
                        with xml.row:
                            xml.SchemaName(resp_row['SchemaName'])
                            xml.SchemaGuid(resp_row['SchemaGuid'])
                            for idx, restriction in enumerate(resp_row['restrictions']['restriction_names']):
                                with xml.Restrictions:
                                    xml.Name(restriction)
                                    xml.Type(resp_row['restrictions']['restriction_types'][idx])

                            xml.RestrictionsMask(resp_row['RestrictionsMask'])

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml

        if request.Restrictions.RestrictionList.SchemaName == "MDSCHEMA_HIERARCHIES" and \
                        request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)

            restriction_names = ['CATALOG_NAME',
                                 'SCHEMA_NAME',
                                 'CUBE_NAME',
                                 'DIMENSION_UNIQUE_NAME',
                                 'HIERARCHY_NAME',
                                 'HIERARCHY_UNIQUE_NAME',
                                 'HIERARCHY_ORIGIN',
                                 'CUBE_SOURCE',
                                 'HIERARCHY_VISIBILITY']
            restriction_types = ['string',
                                 'string',
                                 'string',
                                 'string',
                                 'string',
                                 'string',
                                 'unsignedShort',
                                 'unsignedShort',
                                 'unsignedShort']

            rows = [{'SchemaName': 'MDSCHEMA_HIERARCHIES',
                     'SchemaGuid': 'C8B522DA-5CF3-11CE-ADE5-00AA0044773D',
                     'restrictions': {'restriction_names': restriction_names,
                                      'restriction_types': restriction_types},
                     'RestrictionsMask': '511'
                     }
                    ]

            return generate_resp(rows)


        if request.Restrictions.RestrictionList.SchemaName == 'MDSCHEMA_MEASURES' and \
                        request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)


            restriction_names = ['CATALOG_NAME',
                                 'SCHEMA_NAME',
                                 'CUBE_NAME',
                                 'MEASURE_NAME',
                                 'MEASURE_UNIQUE_NAME',
                                 'MEASUREGROUP_NAME',
                                 'CUBE_SOURCE',
                                 'MEASURE_VISIBILITY']
            restriction_types = ['string',
                                 'string',
                                 'string',
                                 'string',
                                 'string',
                                 'string',
                                 'unsignedShort',
                                 'unsignedShort']

            rows = [{'SchemaName': 'MDSCHEMA_MEASURES',
                     'SchemaGuid': 'C8B522DC-5CF3-11CE-ADE5-00AA0044773D',
                     'restrictions': {'restriction_names': restriction_names,
                                      'restriction_types': restriction_types},
                     'RestrictionsMask': '255'
                     }
                    ]

            return generate_resp(rows)

        # TODO delete
        if request.Properties.PropertyList.Catalog is not None:
            return generate_resp(rows)

        rows = [
                {'SchemaName': 'DBSCHEMA_TABLES',
                 'SchemaGuid': 'C8B52229-5CF3-11CE-ADE5-00AA0044773D',
                 'restrictions': {
                     'restriction_names': ['TABLE_CATALOG',
                                           'TABLE_SCHEMA',
                                           'TABLE_NAME',
                                           'TABLE_TYPE',
                                           'TABLE_OLAP_TYPE'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string']},
                 'RestrictionsMask': '31'
                 },
                {'SchemaName': 'DISCOVER_DATASOURCES',
                 'SchemaGuid': '06C03D41-F66D-49F3-B1B8-987F7AF4CF18',
                 'restrictions': {
                     'restriction_names': ['DataSourceName',
                                           'URL',
                                           'ProviderName',
                                           'ProviderType',
                                           'AuthenticationMode'],
                     'restriction_types': ['string',
                                           'string',
                                           'string',
                                           'string',
                                           'string']},
                 'RestrictionsMask': '31'
                 },
                {'SchemaName': 'DISCOVER_INSTANCES',
                 'SchemaGuid': '20518699-2474-4C15-9885-0E947EC7A7E3',
                 'restrictions': {
                     'restriction_names': ['INSTANCE_NAME'],
                     'restriction_types': ['string']},
                 'RestrictionsMask': '1'
                 },
                {'SchemaName': 'DISCOVER_KEYWORDS',
                 'SchemaGuid': '1426C443-4CDD-4A40-8F45-572FAB9BBAA1',
                 'restrictions': {
                     'restriction_names': ['Keyword'],
                     'restriction_types': ['string']},
                 'RestrictionsMask': '1'
                 }
                ] + rows
        return generate_resp(rows)

    @staticmethod
    def discover_literals_response(request):
        if request.Properties.PropertyList.Content == 'SchemaData' \
                and request.Properties.PropertyList.Format == 'Tabular':

            rows = [{'LiteralName':'DBLITERAL_CATALOG_NAME',
                     'LiteralInvalidChars' : '.',
                     'LiteralInvalidStartingChars' : '0123456789',
                     'LiteralMaxLength' : '24',
                     'LiteralNameEnumValue' : '2'
                     },
                    {'LiteralName': 'DBLITERAL_CATALOG_SEPARATOR',
                     'LiteralValue': '.',
                     'LiteralMaxLength': '0',
                     'LiteralNameEnumValue': '3'
                     },
                    {'LiteralName': 'DBLITERAL_COLUMN_ALIAS',
                     'LiteralInvalidChars': "'&quot;[]",
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '5'
                     },
                    {'LiteralName': 'DBLITERAL_COLUMN_NAME',
                     'LiteralInvalidChars': '.',
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '6'
                     },
                    {'LiteralName': 'DBLITERAL_CORRELATION_NAME',
                     'LiteralInvalidChars': "'&quot;[]",
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '7'
                     },
                    {'LiteralName': 'DBLITERAL_CUBE_NAME',
                     'LiteralInvalidChars': '.',
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '21'
                     },
                    {'LiteralName': 'DBLITERAL_DIMENSION_NAME',
                     'LiteralInvalidChars': '.',
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '22'
                     },
                    {'LiteralName': 'DBLITERAL_LEVEL_NAME',
                     'LiteralInvalidChars': '.',
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '24'
                     },
                    {'LiteralName': 'DBLITERAL_MEMBER_NAME',
                     'LiteralInvalidChars': '.',
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '25'
                     },
                    {'LiteralName': 'DBLITERAL_PROCEDURE_NAME',
                     'LiteralInvalidChars': '.',
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '14'
                     },
                    {'LiteralName': 'DBLITERAL_PROPERTY_NAME',
                     'LiteralInvalidChars': '.',
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '26'
                     },
                    {'LiteralName': 'DBLITERAL_QUOTE_PREFIX',
                     'LiteralValue': '[',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '15'
                     },
                    {'LiteralName': 'DBLITERAL_QUOTE_SUFFIX',
                     'LiteralValue': ']',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '28'
                     },
                    {'LiteralName': 'DBLITERAL_TABLE_NAME',
                     'LiteralInvalidChars': '.',
                     'LiteralInvalidStartingChars': '0123456789',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '17'
                     },
                    {'LiteralName': 'DBLITERAL_TEXT_COMMAND',
                     'LiteralMaxLength': '-1',
                     'LiteralNameEnumValue': '18'
                     },
                    {'LiteralName': 'DBLITERAL_USER_NAME',
                     'LiteralMaxLength': '0',
                     'LiteralNameEnumValue': '19'
                     },
             ]

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(discover_literals_xsd, xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                              **{'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                                 'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'}):

                    for resp_row in rows:
                        with xml.row:
                            for att_name,value in resp_row.items():
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
                xml.root(mdschema_sets_xsd, xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                              **{'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                                 'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'})

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml


    def discover_mdschema_kpis_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue \
                and request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml['return']:
                xml.root(mdschema_kpis_xsd, xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                         **{'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'})

            html_parser = HTMLParser.HTMLParser()
            xml = html_parser.unescape(str(xml))

            return xml


    def discover_dbschema_catalogs_response(self, request):
        xml = xmlwitch.Builder()
        with xml['return']:
            with xml.root(dbschema_catalogs_xsd, xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                     **{'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'}):

                for catalogue in self.catalogues:
                    with xml.row:
                        xml.CATALOG_NAME(catalogue)

        html_parser = HTMLParser.HTMLParser()
        xml = html_parser.unescape(str(xml))

        return xml

    def discover_mdschema_cubes_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue \
                and request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)
            if request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue:
                return etree.fromstring("""
                <return>
                    <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    """ + mdschema_cubes_xsd + """
                        <row>
                            <CATALOG_NAME>{0}</CATALOG_NAME>
                            <CUBE_NAME>{1}</CUBE_NAME>
                            <CUBE_TYPE>CUBE</CUBE_TYPE>
                            <LAST_SCHEMA_UPDATE>2016-07-22T10:41:38</LAST_SCHEMA_UPDATE>
                            <LAST_DATA_UPDATE>2016-07-22T10:41:38</LAST_DATA_UPDATE>
                            <DESCRIPTION>MDX {1} results</DESCRIPTION>
                            <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>
                            <IS_LINKABLE>false</IS_LINKABLE>
                            <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>
                            <IS_SQL_ENABLED>false</IS_SQL_ENABLED>
                            <CUBE_CAPTION>{1}</CUBE_CAPTION>
                            <CUBE_SOURCE>1</CUBE_SOURCE>
                        </row>
                    </root>
                  </return>
                          """.format(self.selected_catalogue,
                                     self.selected_catalogue))
            return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + mdschema_cubes_xsd + """
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <CUBE_NAME>{0}</CUBE_NAME>
                        <CUBE_TYPE>CUBE</CUBE_TYPE>
                        <LAST_SCHEMA_UPDATE>2016-07-25T15:18:20</LAST_SCHEMA_UPDATE>
                        <LAST_DATA_UPDATE>2016-07-25T15:18:20</LAST_DATA_UPDATE>
                        <DESCRIPTION>A demo. cube</DESCRIPTION>
                        <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>
                        <IS_LINKABLE>false</IS_LINKABLE>
                        <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>
                        <IS_SQL_ENABLED>false</IS_SQL_ENABLED>
                        <CUBE_CAPTION>{0}</CUBE_CAPTION>
                        <CUBE_SOURCE>1</CUBE_SOURCE>
                    </row>
                </root>
            </return>""".format(self.selected_catalogue))

        if request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)
            return etree.fromstring("""
                                <return>
            <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + mdschema_cubes_xsd + """
                <row>
                    <CATALOG_NAME>{0}</CATALOG_NAME>
                    <CUBE_NAME>{0}</CUBE_NAME>
                    <CUBE_TYPE>CUBE</CUBE_TYPE>
                    <LAST_SCHEMA_UPDATE>2016-07-25T15:18:20</LAST_SCHEMA_UPDATE>
                    <LAST_DATA_UPDATE>2016-07-25T15:18:20</LAST_DATA_UPDATE>
                    <DESCRIPTION>A demo. cube</DESCRIPTION>
                    <IS_DRILLTHROUGH_ENABLED>true</IS_DRILLTHROUGH_ENABLED>
                    <IS_LINKABLE>false</IS_LINKABLE>
                    <IS_WRITE_ENABLED>false</IS_WRITE_ENABLED>
                    <IS_SQL_ENABLED>false</IS_SQL_ENABLED>
                    <CUBE_CAPTION>{0}</CUBE_CAPTION>
                    <CUBE_SOURCE>1</CUBE_SOURCE>
                </row>
            </root>
        </return>
        """.format(self.selected_catalogue))

    def discover_dbschema_tables_response(self, request):
        if request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)
            return etree.fromstring("""
                <return>
            <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + dbschema_tables_xsd + """
            </root>
        </return>
        """)

    def discover_mdschema_measures__response(self, request):
        measures = ""
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                        request.Restrictions.RestrictionList.MEASURE_VISIBILITY == 3 and\
                        request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            for mes in self.executer.measures:
                measures += """
                <row>
                    <CATALOG_NAME>{0}</CATALOG_NAME>
                    <CUBE_NAME>{1}</CUBE_NAME>
                    <MEASURE_NAME>{2}</MEASURE_NAME>
                    <MEASURE_UNIQUE_NAME>[Measures].[{2}]</MEASURE_UNIQUE_NAME>
                    <MEASURE_CAPTION>{2}</MEASURE_CAPTION>
                    <MEASURE_AGGREGATOR>1</MEASURE_AGGREGATOR>
                    <DATA_TYPE>5</DATA_TYPE>
                    <NUMERIC_PRECISION>16</NUMERIC_PRECISION>
                    <NUMERIC_SCALE>-1</NUMERIC_SCALE>
                    <MEASURE_IS_VISIBLE>true</MEASURE_IS_VISIBLE>
                    <MEASURE_NAME_SQL_COLUMN_NAME>{2}</MEASURE_NAME_SQL_COLUMN_NAME>
                    <MEASURE_UNQUALIFIED_CAPTION>{2}</MEASURE_UNQUALIFIED_CAPTION>
                    <MEASUREGROUP_NAME>default</MEASUREGROUP_NAME>
                </row>
                """.format(self.selected_catalogue, self.selected_catalogue,
                           mes)

            return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + mdschema_measures_xsd + """
                {0}
                </root>
            </return>
            """.format(measures))

    def discover_mdschema_dimensions_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and\
                        request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue and \
                        request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            rows = ""
            ord = 1

            for tables in self.executer.get_all_tables_names(ignore_fact=True):

                rows += """
                <row>
                    <CATALOG_NAME>{0}</CATALOG_NAME>
                    <CUBE_NAME>{0}</CUBE_NAME>
                    <DIMENSION_NAME>{1}</DIMENSION_NAME>
                    <DIMENSION_UNIQUE_NAME>[{1}]</DIMENSION_UNIQUE_NAME>
                    <DIMENSION_CAPTION>{1}</DIMENSION_CAPTION>
                    <DIMENSION_ORDINAL>{2}</DIMENSION_ORDINAL>
                    <DIMENSION_TYPE>3</DIMENSION_TYPE>
                    <DIMENSION_CARDINALITY>23</DIMENSION_CARDINALITY>
                    <DEFAULT_HIERARCHY>[{1}].[{1}]</DEFAULT_HIERARCHY>
                    <IS_VIRTUAL>false</IS_VIRTUAL>
                    <IS_READWRITE>false</IS_READWRITE>
                    <DIMENSION_UNIQUE_SETTINGS>1</DIMENSION_UNIQUE_SETTINGS>
                    <DIMENSION_IS_VISIBLE>true</DIMENSION_IS_VISIBLE>
                </row>""".format(self.selected_catalogue, tables, ord)
                ord += 1

            rows += """
            <row>
                <CATALOG_NAME>{0}</CATALOG_NAME>
                <CUBE_NAME>{0}</CUBE_NAME>
                <DIMENSION_NAME>Measures</DIMENSION_NAME>
                <DIMENSION_UNIQUE_NAME>[Measures]</DIMENSION_UNIQUE_NAME>
                <DIMENSION_CAPTION>Measures</DIMENSION_CAPTION>
                <DIMENSION_ORDINAL>{1}</DIMENSION_ORDINAL>
                <DIMENSION_TYPE>2</DIMENSION_TYPE>
                <DIMENSION_CARDINALITY>0</DIMENSION_CARDINALITY>
                <DEFAULT_HIERARCHY>[Measures]</DEFAULT_HIERARCHY>
                <IS_VIRTUAL>false</IS_VIRTUAL>
                <IS_READWRITE>false</IS_READWRITE>
                <DIMENSION_UNIQUE_SETTINGS>1</DIMENSION_UNIQUE_SETTINGS>
                <DIMENSION_IS_VISIBLE>true</DIMENSION_IS_VISIBLE>
            </row>""".format(self.selected_catalogue, ord)

            return etree.fromstring("""
                <return>
                    <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                      """ + mdschema_dimensions_xsd + """
                      {0}
                    </root>
              </return>
                    """.format(rows))

    def discover_mdschema_hierarchies_response(self, request):

        # Enumeration of hierarchies in all dimensions
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                        request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            if request.Restrictions.RestrictionList.HIERARCHY_VISIBILITY == 3:
                rows = ""
                for table_name, df in self.executer.tables_loaded.items():
                    if table_name == self.executer.facts:
                        continue

                    # french caracteres
                    # TODO encode dataframe
                    if type(df.iloc[0][0]) == unicode:
                        column_attribut = df.iloc[0][0].encode('utf-8',
                                                               'replace')
                    else:
                        column_attribut = df.iloc[0][0]

                    rows += """
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <CUBE_NAME>{0}</CUBE_NAME>
                        <DIMENSION_UNIQUE_NAME>[{1}]</DIMENSION_UNIQUE_NAME>
                        <HIERARCHY_NAME>{1}</HIERARCHY_NAME>
                        <HIERARCHY_UNIQUE_NAME>[{1}].[{1}]</HIERARCHY_UNIQUE_NAME>
                        <HIERARCHY_CAPTION>{1}</HIERARCHY_CAPTION>
                        <DIMENSION_TYPE>3</DIMENSION_TYPE>
                        <HIERARCHY_CARDINALITY>6</HIERARCHY_CARDINALITY>
                        <DEFAULT_MEMBER>[{1}].[{1}].[{2}].[{3}]</DEFAULT_MEMBER>
                        <STRUCTURE>0</STRUCTURE>
                        <IS_VIRTUAL>false</IS_VIRTUAL>
                        <IS_READWRITE>false</IS_READWRITE>
                        <DIMENSION_UNIQUE_SETTINGS>1</DIMENSION_UNIQUE_SETTINGS>
                        <DIMENSION_IS_VISIBLE>true</DIMENSION_IS_VISIBLE>
                        <HIERARCHY_ORDINAL>1</HIERARCHY_ORDINAL>
                        <DIMENSION_IS_SHARED>true</DIMENSION_IS_SHARED>
                        <HIERARCHY_IS_VISIBLE>true</HIERARCHY_IS_VISIBLE>
                        <HIERARCHY_ORIGIN>1</HIERARCHY_ORIGIN>
                        <INSTANCE_SELECTION>0</INSTANCE_SELECTION>
                    </row>
                    """.format(self.selected_catalogue, table_name,
                               df.columns[0], column_attribut)

                rows += """
                <row>
                    <CATALOG_NAME>{0}</CATALOG_NAME>
                    <CUBE_NAME>{0}</CUBE_NAME>
                    <DIMENSION_UNIQUE_NAME>[Measures]</DIMENSION_UNIQUE_NAME>
                    <HIERARCHY_NAME>Measures</HIERARCHY_NAME>
                    <HIERARCHY_UNIQUE_NAME>[Measures]</HIERARCHY_UNIQUE_NAME>
                    <HIERARCHY_CAPTION>Measures</HIERARCHY_CAPTION>
                    <DIMENSION_TYPE>2</DIMENSION_TYPE>
                    <HIERARCHY_CARDINALITY>0</HIERARCHY_CARDINALITY>
                    <DEFAULT_MEMBER>[Measures].[{1}]</DEFAULT_MEMBER>
                    <STRUCTURE>0</STRUCTURE>
                    <IS_VIRTUAL>false</IS_VIRTUAL>
                    <IS_READWRITE>false</IS_READWRITE>
                    <DIMENSION_UNIQUE_SETTINGS>1</DIMENSION_UNIQUE_SETTINGS>
                    <DIMENSION_IS_VISIBLE>true</DIMENSION_IS_VISIBLE>
                    <HIERARCHY_ORDINAL>1</HIERARCHY_ORDINAL>
                    <DIMENSION_IS_SHARED>true</DIMENSION_IS_SHARED>
                    <HIERARCHY_IS_VISIBLE>true</HIERARCHY_IS_VISIBLE>
                    <HIERARCHY_ORIGIN>1</HIERARCHY_ORIGIN>
                    <INSTANCE_SELECTION>0</INSTANCE_SELECTION>
                </row>
                """.format(self.selected_catalogue, self.executer.measures[0])

                return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + mdschema_hierarchies_xsd + """
                    {0}
                </root>
            </return>
            """.format(rows))

            if request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue:
                rows = ""
                for table_name, df in self.executer.tables_loaded.items():
                    if table_name == self.executer.facts:
                        continue

                    # french caracteres
                    # TODO encode dataframe
                    if type(df.iloc[0][0]) == unicode:
                        column_attribut = df.iloc[0][0].encode('utf-8',
                                                               'replace')
                    else:
                        column_attribut = df.iloc[0][0]

                    rows += """
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <CUBE_NAME>{0}</CUBE_NAME>
                        <DIMENSION_UNIQUE_NAME>[{1}]</DIMENSION_UNIQUE_NAME>
                        <HIERARCHY_NAME>{1}</HIERARCHY_NAME>
                        <HIERARCHY_UNIQUE_NAME>[{1}].[{1}]</HIERARCHY_UNIQUE_NAME>
                        <HIERARCHY_CAPTION>{1}</HIERARCHY_CAPTION>
                        <DIMENSION_TYPE>3</DIMENSION_TYPE>
                        <HIERARCHY_CARDINALITY>6</HIERARCHY_CARDINALITY>
                        <DEFAULT_MEMBER>[{1}].[{1}].[{2}].[{3}]</DEFAULT_MEMBER>
                        <STRUCTURE>0</STRUCTURE>
                        <IS_VIRTUAL>false</IS_VIRTUAL>
                        <IS_READWRITE>false</IS_READWRITE>
                        <DIMENSION_UNIQUE_SETTINGS>1</DIMENSION_UNIQUE_SETTINGS>
                        <DIMENSION_IS_VISIBLE>true</DIMENSION_IS_VISIBLE>
                        <HIERARCHY_ORDINAL>1</HIERARCHY_ORDINAL>
                        <DIMENSION_IS_SHARED>true</DIMENSION_IS_SHARED>
                        <HIERARCHY_IS_VISIBLE>true</HIERARCHY_IS_VISIBLE>
                        <HIERARCHY_ORIGIN>1</HIERARCHY_ORIGIN>
                        <INSTANCE_SELECTION>0</INSTANCE_SELECTION>
                    </row>
                        """.format(self.selected_catalogue, table_name,
                                   df.columns[0], column_attribut)

                rows += """
                <row>
                    <CATALOG_NAME>{0}</CATALOG_NAME>
                    <CUBE_NAME>{0}</CUBE_NAME>
                    <DIMENSION_UNIQUE_NAME>[Measures]</DIMENSION_UNIQUE_NAME>
                    <HIERARCHY_NAME>Measures</HIERARCHY_NAME>
                    <HIERARCHY_UNIQUE_NAME>[Measures]</HIERARCHY_UNIQUE_NAME>
                    <HIERARCHY_CAPTION>Measures</HIERARCHY_CAPTION>
                    <DIMENSION_TYPE>2</DIMENSION_TYPE>
                    <HIERARCHY_CARDINALITY>0</HIERARCHY_CARDINALITY>
                    <DEFAULT_MEMBER>[Measures].[{1}]</DEFAULT_MEMBER>
                    <STRUCTURE>0</STRUCTURE>
                    <IS_VIRTUAL>false</IS_VIRTUAL>
                    <IS_READWRITE>false</IS_READWRITE>
                    <DIMENSION_UNIQUE_SETTINGS>1</DIMENSION_UNIQUE_SETTINGS>
                    <DIMENSION_IS_VISIBLE>true</DIMENSION_IS_VISIBLE>
                    <HIERARCHY_ORDINAL>1</HIERARCHY_ORDINAL>
                    <DIMENSION_IS_SHARED>true</DIMENSION_IS_SHARED>
                    <HIERARCHY_IS_VISIBLE>true</HIERARCHY_IS_VISIBLE>
                    <HIERARCHY_ORIGIN>1</HIERARCHY_ORIGIN>
                    <INSTANCE_SELECTION>0</INSTANCE_SELECTION>
                </row>""".format(self.selected_catalogue,
                                 self.executer.measures[0])
                return etree.fromstring("""
                <return>
                    <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    """ + mdschema_hierarchies_xsd + """
                        {0}
                    </root>
                 </return>""".format(rows))

    def discover_mdschema_levels__response(self, request):
        # TODO fix levels in the same table (with xml file maybe) !!!!!!!!!
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            rows = ""
            for tables in self.executer.get_all_tables_names(ignore_fact=True):
                l_nb = 0
                for col in self.executer.tables_loaded[tables].columns:
                    rows += """
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <CUBE_NAME>{0}</CUBE_NAME>
                        <DIMENSION_UNIQUE_NAME>[{1}]</DIMENSION_UNIQUE_NAME>
                        <HIERARCHY_UNIQUE_NAME>[{1}].[{1}]</HIERARCHY_UNIQUE_NAME>
                        <LEVEL_NAME>{2}</LEVEL_NAME>
                        <LEVEL_UNIQUE_NAME>[{1}].[{1}].[{2}]</LEVEL_UNIQUE_NAME>
                        <LEVEL_CAPTION>{2}</LEVEL_CAPTION>
                        <LEVEL_NUMBER>{3}</LEVEL_NUMBER>
                        <LEVEL_CARDINALITY>1</LEVEL_CARDINALITY>
                        <LEVEL_TYPE>0</LEVEL_TYPE>
                        <CUSTOM_ROLLUP_SETTINGS>0</CUSTOM_ROLLUP_SETTINGS>
                        <LEVEL_UNIQUE_SETTINGS>0</LEVEL_UNIQUE_SETTINGS>
                        <LEVEL_IS_VISIBLE>true</LEVEL_IS_VISIBLE>
                        <LEVEL_DBTYPE>130</LEVEL_DBTYPE>
                        <LEVEL_KEY_CARDINALITY>1</LEVEL_KEY_CARDINALITY>
                        <LEVEL_ORIGIN>2</LEVEL_ORIGIN>
                    </row>
                    """.format(self.selected_catalogue, tables, col, l_nb)
                    l_nb += 1

            rows += """
            <row>
                <CATALOG_NAME>{0}</CATALOG_NAME>
                <CUBE_NAME>{0}</CUBE_NAME>
                <DIMENSION_UNIQUE_NAME>[Measures]</DIMENSION_UNIQUE_NAME>
                <HIERARCHY_UNIQUE_NAME>[Measures]</HIERARCHY_UNIQUE_NAME>
                <LEVEL_NAME>MeasuresLevel</LEVEL_NAME>
                <LEVEL_UNIQUE_NAME>[Measures]</LEVEL_UNIQUE_NAME>
                <LEVEL_CAPTION>MeasuresLevel</LEVEL_CAPTION>
                <LEVEL_NUMBER>0</LEVEL_NUMBER>
                <LEVEL_CARDINALITY>0</LEVEL_CARDINALITY>
                <LEVEL_TYPE>0</LEVEL_TYPE>
                <CUSTOM_ROLLUP_SETTINGS>0</CUSTOM_ROLLUP_SETTINGS>
                <LEVEL_UNIQUE_SETTINGS>0</LEVEL_UNIQUE_SETTINGS>
                <LEVEL_IS_VISIBLE>true</LEVEL_IS_VISIBLE>
                <LEVEL_DBTYPE>130</LEVEL_DBTYPE>
                <LEVEL_KEY_CARDINALITY>1</LEVEL_KEY_CARDINALITY>
                <LEVEL_ORIGIN>2</LEVEL_ORIGIN>
            </row>
            """.format(self.selected_catalogue)

            return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + mdschema_levels_xsd + """
                {0}
                </root>
            </return>""".format(rows))

    def discover_mdschema_measuresgroups_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + mdschema_measuresgroups_xsd + """
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <CUBE_NAME>{0}</CUBE_NAME>
                        <MEASUREGROUP_NAME>default</MEASUREGROUP_NAME>
                        <DESCRIPTION>-</DESCRIPTION>
                        <IS_WRITE_ENABLED>true</IS_WRITE_ENABLED>
                        <MEASUREGROUP_CAPTION>default</MEASUREGROUP_CAPTION>
                    </row>
                </root>
            </return>
            """.format(self.selected_catalogue))

    def discover_mdschema_measuresgroups_dimensions_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            rows = ""
            for tables in self.executer.get_all_tables_names(ignore_fact=True):
                rows += """
                <row>
                    <CATALOG_NAME>{0}</CATALOG_NAME>
                    <CUBE_NAME>{0}</CUBE_NAME>
                    <MEASUREGROUP_NAME>default</MEASUREGROUP_NAME>
                    <MEASUREGROUP_CARDINALITY>ONE</MEASUREGROUP_CARDINALITY>
                    <DIMENSION_UNIQUE_NAME>[{1}]</DIMENSION_UNIQUE_NAME>
                    <DIMENSION_CARDINALITY>MANY</DIMENSION_CARDINALITY>
                    <DIMENSION_IS_VISIBLE>true</DIMENSION_IS_VISIBLE>
                    <DIMENSION_IS_FACT_DIMENSION>false</DIMENSION_IS_FACT_DIMENSION>
                    <DIMENSION_GRANULARITY>[{1}].[{1}]</DIMENSION_GRANULARITY>
                </row>
                """.format(self.selected_catalogue, tables)

            return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                  """ + mdschema_measuresgroups_dimensions_xsd + """
                {0}
              </root>
            </return>
            """.format(rows))

    def discover_mdschema_properties_response(self, request):

        if request.Restrictions.RestrictionList.PROPERTY_TYPE == 2 and \
            request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                {1}
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>FONT_FLAGS</PROPERTY_NAME>
                        <PROPERTY_CAPTION>FONT_FLAGS</PROPERTY_CAPTION>
                        <DATA_TYPE>3</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>LANGUAGE</PROPERTY_NAME>
                        <PROPERTY_CAPTION>LANGUAGE</PROPERTY_CAPTION>
                        <DATA_TYPE>19</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>style</PROPERTY_NAME>
                        <PROPERTY_CAPTION>style</PROPERTY_CAPTION>
                        <DATA_TYPE>130</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>ACTION_TYPE</PROPERTY_NAME>
                        <PROPERTY_CAPTION>ACTION_TYPE</PROPERTY_CAPTION>
                        <DATA_TYPE>19</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>FONT_SIZE</PROPERTY_NAME>
                        <PROPERTY_CAPTION>FONT_SIZE</PROPERTY_CAPTION>
                        <DATA_TYPE>18</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>FORMAT_STRING</PROPERTY_NAME>
                        <PROPERTY_CAPTION>FORMAT_STRING</PROPERTY_CAPTION>
                        <DATA_TYPE>130</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>className</PROPERTY_NAME>
                        <PROPERTY_CAPTION>className</PROPERTY_CAPTION>
                        <DATA_TYPE>130</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>UPDATEABLE</PROPERTY_NAME>
                        <PROPERTY_CAPTION>UPDATEABLE</PROPERTY_CAPTION>
                        <DATA_TYPE>19</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>BACK_COLOR</PROPERTY_NAME>
                        <PROPERTY_CAPTION>BACK_COLOR</PROPERTY_CAPTION>
                        <DATA_TYPE>19</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>CELL_ORDINAL</PROPERTY_NAME>
                        <PROPERTY_CAPTION>CELL_ORDINAL</PROPERTY_CAPTION>
                        <DATA_TYPE>19</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>FONT_NAME</PROPERTY_NAME>
                        <PROPERTY_CAPTION>FONT_NAME</PROPERTY_CAPTION>
                        <DATA_TYPE>130</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>VALUE</PROPERTY_NAME>
                        <PROPERTY_CAPTION>VALUE</PROPERTY_CAPTION>
                        <DATA_TYPE>12</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>FORMATTED_VALUE</PROPERTY_NAME>
                        <PROPERTY_CAPTION>FORMATTED_VALUE</PROPERTY_CAPTION>
                        <DATA_TYPE>130</DATA_TYPE>
                    </row>
                    <row>
                        <CATALOG_NAME>{0}</CATALOG_NAME>
                        <PROPERTY_TYPE>2</PROPERTY_TYPE>
                        <PROPERTY_NAME>FORE_COLOR</PROPERTY_NAME>
                        <PROPERTY_CAPTION>FORE_COLOR</PROPERTY_CAPTION>
                        <DATA_TYPE>19</DATA_TYPE>
                    </row>
                    </root>
                  </return>
            """.format(self.selected_catalogue,
                       mdschema_properties_properties_xsd))

        elif request.Restrictions.RestrictionList.PROPERTY_TYPE == 1:
            return etree.fromstring("""
              <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                 {0}
                </root>
              </return>
            """.format(mdschema_properties_properties_xsd))

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
            return etree.fromstring("""
        <return>
            <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            """ + mdschema_members_xsd + """
                <row>
                    <CATALOG_NAME>{0}</CATALOG_NAME>
                    <CUBE_NAME>{0}</CUBE_NAME>
                    <DIMENSION_UNIQUE_NAME>{1}</DIMENSION_UNIQUE_NAME>
                    <HIERARCHY_UNIQUE_NAME>{1}.{1}</HIERARCHY_UNIQUE_NAME>
                    <LEVEL_UNIQUE_NAME>{2}</LEVEL_UNIQUE_NAME>
                    <LEVEL_NUMBER>0</LEVEL_NUMBER>
                    <MEMBER_ORDINAL>0</MEMBER_ORDINAL>
                    <MEMBER_NAME>""" + last_attribut + """</MEMBER_NAME>
                    <MEMBER_UNIQUE_NAME>{3}</MEMBER_UNIQUE_NAME>
                    <MEMBER_TYPE>1</MEMBER_TYPE>
                    <MEMBER_CAPTION>""" + last_attribut + """</MEMBER_CAPTION>
                    <CHILDREN_CARDINALITY>1</CHILDREN_CARDINALITY>
                    <PARENT_LEVEL>0</PARENT_LEVEL>
                    <PARENT_COUNT>0</PARENT_COUNT>
                    <MEMBER_KEY>""" + last_attribut + """</MEMBER_KEY>
                    <IS_PLACEHOLDERMEMBER>false</IS_PLACEHOLDERMEMBER>
                    <IS_DATAMEMBER>false</IS_DATAMEMBER>
                </row>
            </root>
        </return>
            """.format(
                self.selected_catalogue, separed_tuple[0], joined,
                request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME))
