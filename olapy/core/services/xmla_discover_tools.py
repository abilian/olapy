"""
Managing all `DISCOVER <https://technet.microsoft.com/fr-fr/library/ms186653(v=sql.110).aspx>`_ requests and responses
"""
# -*- encoding: utf8 -*-

from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
import uuid

import xmlwitch

from olapy.core.services.xmla_discover_tools_utils import discover_literals_response_rows, \
    discover_schema_rowsets_response_rows

from ..mdx.executor.execute import MdxEngine
from .xmla_discover_xsds import dbschema_catalogs_xsd, dbschema_tables_xsd, \
    discover_datasources_xsd, discover_literals_xsd, discover_preperties_xsd, \
    discover_schema_rowsets_xsd, mdschema_cubes_xsd, mdschema_dimensions_xsd, \
    mdschema_hierarchies_xsd, mdschema_kpis_xsd, mdschema_levels_xsd, \
    mdschema_measures_xsd, mdschema_measuresgroups_dimensions_xsd, \
    mdschema_measuresgroups_xsd, mdschema_members_xsd, \
    mdschema_properties_properties_xsd, mdschema_sets_xsd


# noinspection PyPep8Naming
class XmlaTools():
    """XmlaDiscoverTools for generating xmla discover responses."""

    def __init__(self, source_type, db_config, cubes_config, **kwargs):
        # right now the catalogue_name and cube name are the same
        executor = kwargs.get('executor', None)
        olapy_data = kwargs.get('olapy_data', None)

        # todo change
        mdx_executor = MdxEngine(olapy_data_location=olapy_data, source_type=source_type, database_config=db_config,
                                 cube_config=cubes_config)
        self.catalogues = mdx_executor.get_cubes_names()

        # todo change catalogue here
        if executor and cubes_config:
            facts = cubes_config.facts[0].table_name
        else:
            facts = 'Facts'

        # todo directly from xmla.py
        if self.catalogues:
            self.selected_catalogue = self.catalogues[0]
            if executor:
                self.executor = executor
            else:
                mdx_executor.load_cube(self.selected_catalogue, fact_table_name=facts)
                self.executor = mdx_executor

        self.session_id = uuid.uuid1()

    def change_catalogue(self, new_catalogue):
        """
        If you change the catalogue (cube) in any request, we have to
        instantiate the MdxEngine with the new catalogue.

        :param new_catalogue: catalogue name
        :return: new instance of MdxEngine with new star_schema_DataFrame and other variables
        """

        if self.selected_catalogue != new_catalogue:
            if self.executor.cube_config and new_catalogue == self.executor.cube_config.name:
                facts = self.executor.cube_config.facts[0].table_name
            else:
                facts = 'Facts'

            self.selected_catalogue = new_catalogue
            self.executor.load_cube(new_catalogue, fact_table_name=facts)

    @staticmethod
    def discover_datasources_response():
        xml = xmlwitch.Builder()
        with xml['return']:
            with xml.root(
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        'xmlns:EX':
                        'urn:schemas-microsoft-com:xml-analysis:exception',
                        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi':
                        'http://www.w3.org/2001/XMLSchema-instance',
                    }):
                xml.write(discover_datasources_xsd)
                with xml.row:
                    xml.DataSourceName('sales')
                    xml.DataSourceDescription('sales Sample Data')
                    xml.URL('http://127.0.0.1:8000/xmla')
                    xml.DataSourceInfo('-')
                    xml.ProviderName('olapy')
                    xml.ProviderType('MDP')
                    xml.AuthenticationMode('Unauthenticated')

        return str(xml)

    @staticmethod
    def _get_props(
            xsd,
            PropertyName,
            PropertyDescription,
            PropertyType,
            PropertyAccessType,
            IsRequired,
            Value,
    ):
        xml = xmlwitch.Builder()

        if PropertyName is not '':
            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(xsd)
                    with xml.row:
                        xml.PropertyName(PropertyName)
                        xml.PropertyDescription(PropertyDescription)
                        xml.PropertyType(PropertyType)
                        xml.PropertyAccessType(PropertyAccessType)
                        xml.IsRequired(IsRequired)
                        xml.Value(Value)

        else:
            properties_names_n_description = [
                'ServerName',
                'ProviderVersion',
                'MdpropMdxSubqueries',
                'MdpropMdxDrillFunctions',
                'MdpropMdxNamedSets',
            ]
            properties_types = ['string', 'string', 'int', 'int', 'int']
            values = [
                os.getenv('USERNAME', 'default'),
                '0.0.3  25-Nov-2016 07:20:28 GMT',
                '15',
                '3',
                '15',
            ]

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(xsd)
                    for idx, prop_desc in enumerate(
                            properties_names_n_description,):
                        with xml.row:
                            xml.PropertyName(prop_desc)
                            xml.PropertyDescription(prop_desc)
                            xml.PropertyType(properties_types[idx])
                            xml.PropertyAccessType('Read')
                            xml.IsRequired('false')
                            xml.Value(values[idx])

        return str(xml)

    def discover_properties_response(self, request):

        if request.Restrictions.RestrictionList.PropertyName == 'Catalog':
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(
                    request.Properties.PropertyList.Catalog.replace(
                        '[',
                        '',
                    ).replace(']', ''),)
                value = self.selected_catalogue
            else:
                value = "olapy Unspecified Catalog"

            return self._get_props(
                discover_preperties_xsd,
                'Catalog',
                'Catalog',
                'string',
                'ReadWrite',
                'false',
                value,
            )

        elif request.Restrictions.RestrictionList.PropertyName == 'ServerName':
            return self._get_props(
                discover_preperties_xsd,
                'ServerName',
                'ServerName',
                'string',
                'Read',
                'false',
                'Mouadh',
            )

        elif request.Restrictions.RestrictionList.PropertyName == 'ProviderVersion':
            return self._get_props(
                discover_preperties_xsd,
                'ProviderVersion',
                'ProviderVersion',
                'string',
                'Read',
                'false',
                '0.02  08-Mar-2016 08:41:28 GMT',
            )

        elif (request.Restrictions.RestrictionList.PropertyName ==
              'MdpropMdxSubqueries'):

            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
            return self._get_props(
                discover_preperties_xsd,
                'MdpropMdxSubqueries',
                'MdpropMdxSubqueries',
                'int',
                'Read',
                'false',
                '15',
            )

        elif request.Restrictions.RestrictionList.PropertyName == 'MdpropMdxDrillFunctions':

            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
            return self._get_props(
                discover_preperties_xsd,
                'MdpropMdxDrillFunctions',
                'MdpropMdxDrillFunctions',
                'int',
                'Read',
                'false',
                '3',
            )

        elif request.Restrictions.RestrictionList.PropertyName == 'MdpropMdxNamedSets':
            return self._get_props(
                discover_preperties_xsd,
                'MdpropMdxNamedSets',
                'MdpropMdxNamedSets',
                'int',
                'Read',
                'false',
                '15',
            )

        return self._get_props(discover_preperties_xsd, '', '', '', '', '', '')

    def discover_schema_rowsets_response(self, request):

        rows = discover_schema_rowsets_response_rows

        def generate_resp(rows):
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(discover_schema_rowsets_xsd)
                    for resp_row in rows:
                        with xml.row:
                            xml.SchemaName(resp_row['SchemaName'])
                            xml.SchemaGuid(resp_row['SchemaGuid'])
                            for idx, restriction in enumerate(
                                    resp_row['restrictions'][
                                        'restriction_names'],):
                                with xml.Restrictions:
                                    xml.Name(restriction)
                                    xml.Type(resp_row['restrictions'][
                                        'restriction_types'][idx])

                            xml.RestrictionsMask(resp_row['RestrictionsMask'])

            return str(xml)

        restriction_list = request.Restrictions.RestrictionList
        if restriction_list.SchemaName == "MDSCHEMA_HIERARCHIES" \
                and request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)

            restriction_names = [
                'CATALOG_NAME',
                'SCHEMA_NAME',
                'CUBE_NAME',
                'DIMENSION_UNIQUE_NAME',
                'HIERARCHY_NAME',
                'HIERARCHY_UNIQUE_NAME',
                'HIERARCHY_ORIGIN',
                'CUBE_SOURCE',
                'HIERARCHY_VISIBILITY',
            ]
            restriction_types = [
                'string',
                'string',
                'string',
                'string',
                'string',
                'string',
                'unsignedShort',
                'unsignedShort',
                'unsignedShort',
            ]

            rows = [{
                'SchemaName': 'MDSCHEMA_HIERARCHIES',
                'SchemaGuid': 'C8B522DA-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': restriction_names,
                    'restriction_types': restriction_types,
                },
                'RestrictionsMask': '511',
            }]

            return generate_resp(rows)

        if restriction_list.SchemaName == 'MDSCHEMA_MEASURES' \
                and request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)

            restriction_names = [
                'CATALOG_NAME',
                'SCHEMA_NAME',
                'CUBE_NAME',
                'MEASURE_NAME',
                'MEASURE_UNIQUE_NAME',
                'MEASUREGROUP_NAME',
                'CUBE_SOURCE',
                'MEASURE_VISIBILITY',
            ]
            restriction_types = [
                'string',
                'string',
                'string',
                'string',
                'string',
                'string',
                'unsignedShort',
                'unsignedShort',
            ]

            rows = [{
                'SchemaName': 'MDSCHEMA_MEASURES',
                'SchemaGuid': 'C8B522DC-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': restriction_names,
                    'restriction_types': restriction_types,
                },
                'RestrictionsMask': '255',
            }]

            return generate_resp(rows)

        ext = [
            {
                'SchemaName': 'DBSCHEMA_TABLES',
                'SchemaGuid': 'C8B52229-5CF3-11CE-ADE5-00AA0044773D',
                'restrictions': {
                    'restriction_names': [
                        'TABLE_CATALOG',
                        'TABLE_SCHEMA',
                        'TABLE_NAME',
                        'TABLE_TYPE',
                        'TABLE_OLAP_TYPE',
                    ],
                    'restriction_types':
                    ['string', 'string', 'string', 'string', 'string'],
                },
                'RestrictionsMask': '31',
            },
            {
                'SchemaName': 'DISCOVER_DATASOURCES',
                'SchemaGuid': '06C03D41-F66D-49F3-B1B8-987F7AF4CF18',
                'restrictions': {
                    'restriction_names': [
                        'DataSourceName',
                        'URL',
                        'ProviderName',
                        'ProviderType',
                        'AuthenticationMode',
                    ],
                    'restriction_types':
                    ['string', 'string', 'string', 'string', 'string'],
                },
                'RestrictionsMask': '31',
            },
            {
                'SchemaName': 'DISCOVER_INSTANCES',
                'SchemaGuid': '20518699-2474-4C15-9885-0E947EC7A7E3',
                'restrictions': {
                    'restriction_names': ['INSTANCE_NAME'],
                    'restriction_types': ['string'],
                },
                'RestrictionsMask': '1',
            },
            {
                'SchemaName': 'DISCOVER_KEYWORDS',
                'SchemaGuid': '1426C443-4CDD-4A40-8F45-572FAB9BBAA1',
                'restrictions': {
                    'restriction_names': ['Keyword'],
                    'restriction_types': ['string'],
                },
                'RestrictionsMask': '1',
            },
        ]

        ext.extend(rows)

        return generate_resp(ext)

    @staticmethod
    def discover_literals_response(request):
        if request.Properties.PropertyList.Content == 'SchemaData' \
                and request.Properties.PropertyList.Format == 'Tabular':

            rows = discover_literals_response_rows

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(discover_literals_xsd)
                    for resp_row in rows:
                        with xml.row:
                            for att_name, value in resp_row.items():
                                xml[att_name](value)

            return str(xml)

    def mdschema_sets_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue \
                and request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_sets_xsd)

            return str(xml)

    def mdschema_kpis_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue \
                and request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_kpis_xsd)

            return str(xml)

    def dbschema_catalogs_response(self, request):
        xml = xmlwitch.Builder()
        with xml['return']:
            with xml.root(
                    xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                    **{
                        'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                        'xmlns:xsi':
                        'http://www.w3.org/2001/XMLSchema-instance',
                    }):
                xml.write(dbschema_catalogs_xsd)
                for catalogue in self.catalogues:
                    with xml.row:
                        xml.CATALOG_NAME(catalogue)

        return str(xml)

    def mdschema_cubes_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue \
                or request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_cubes_xsd)
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_catalogue)
                        xml.CUBE_NAME(self.selected_catalogue)
                        xml.CUBE_TYPE('CUBE')
                        xml.LAST_SCHEMA_UPDATE('2016-07-22T10:41:38')
                        xml.LAST_DATA_UPDATE('2016-07-22T10:41:38')
                        xml.DESCRIPTION(
                            'MDX ' + self.selected_catalogue + ' results',)
                        xml.IS_DRILLTHROUGH_ENABLED('true')
                        xml.IS_LINKABLE('false')
                        xml.IS_WRITE_ENABLED('false')
                        xml.IS_SQL_ENABLED('false')
                        xml.CUBE_CAPTION(self.selected_catalogue)
                        xml.CUBE_SOURCE('1')

            return str(xml)

    def dbschema_tables_response(self, request):
        if request.Properties.PropertyList.Catalog is not None:
            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()
            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(dbschema_tables_xsd)

            return str(xml)

    def mdschema_measures_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_measures_xsd)
                    for mes in self.executor.measures:
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

            return str(xml)

    def mdschema_dimensions_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and\
                request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            ord = 1
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_dimensions_xsd)
                    for tables in self.executor.get_all_tables_names(
                            ignore_fact=True,):
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_catalogue)
                            xml.CUBE_NAME(self.selected_catalogue)
                            xml.DIMENSION_NAME(tables)
                            xml.DIMENSION_UNIQUE_NAME('[' + tables + ']')
                            xml.DIMENSION_CAPTION(tables)
                            xml.DIMENSION_ORDINAL(str(ord))
                            xml.DIMENSION_TYPE('3')
                            xml.DIMENSION_CARDINALITY('23')
                            xml.DEFAULT_HIERARCHY(
                                '[' + tables + '].[' + tables + ']',)
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

            return str(xml)

    def mdschema_hierarchies_response(self, request):

        # Enumeration of hierarchies in all dimensions
        restriction_list = request.Restrictions.RestrictionList

        if restriction_list.CUBE_NAME == self.selected_catalogue \
                and request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            if restriction_list.HIERARCHY_VISIBILITY == 3 \
                    or restriction_list.CATALOG_NAME == self.selected_catalogue:

                xml = xmlwitch.Builder()

                with xml['return']:
                    with xml.root(
                            xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                            **{
                                'xmlns:xsd':
                                'http://www.w3.org/2001/XMLSchema',
                                'xmlns:xsi':
                                'http://www.w3.org/2001/XMLSchema-instance',
                            }):
                        xml.write(mdschema_hierarchies_xsd)
                        for table_name, df in self.executor.tables_loaded.items(
                        ):
                            if table_name == self.executor.facts:
                                continue

                            column_attribut = df.iloc[0][0]

                            with xml.row:
                                xml.CATALOG_NAME(self.selected_catalogue)
                                xml.CUBE_NAME(self.selected_catalogue)
                                xml.DIMENSION_UNIQUE_NAME(
                                    '[' + table_name + ']',)
                                xml.HIERARCHY_NAME(table_name)
                                xml.HIERARCHY_UNIQUE_NAME(
                                    '[{0}].[{0}]'.format(table_name),)
                                xml.HIERARCHY_CAPTION(table_name)
                                xml.DIMENSION_TYPE('3')
                                xml.HIERARCHY_CARDINALITY('6')
                                xml.DEFAULT_MEMBER(
                                    '[{0}].[{0}].[{1}].[{2}]'.format(
                                        table_name,
                                        df.columns[0],
                                        column_attribut,
                                    ),)
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
                            xml.DEFAULT_MEMBER('[Measures].[{}]'.format(
                                self.executor.measures[0],))
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

                return str(xml)

    def mdschema_levels_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):

                    xml.write(mdschema_levels_xsd)
                    for tables in self.executor.get_all_tables_names(
                            ignore_fact=True,):
                        l_nb = 0
                        for col in self.executor.tables_loaded[tables].columns:

                            with xml.row:
                                xml.CATALOG_NAME(self.selected_catalogue)
                                xml.CUBE_NAME(self.selected_catalogue)
                                xml.DIMENSION_UNIQUE_NAME('[' + tables + ']')
                                xml.HIERARCHY_UNIQUE_NAME(
                                    '[{0}].[{0}]'.format(tables),)
                                xml.LEVEL_NAME(str(col))
                                xml.LEVEL_UNIQUE_NAME(
                                    '[{0}].[{0}].[{1}]'.format(tables, col),)
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

            return str(xml)

    def mdschema_measuregroups_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_measuresgroups_xsd)
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_catalogue)
                        xml.CUBE_NAME(self.selected_catalogue)
                        xml.MEASUREGROUP_NAME('default')
                        xml.DESCRIPTION('-')
                        xml.IS_WRITE_ENABLED('true')
                        xml.MEASUREGROUP_CAPTION('default')

            return str(xml)

    def mdschema_measuregroup_dimensions_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None:

            self.change_catalogue(request.Properties.PropertyList.Catalog)
            # rows = ""
            xml = xmlwitch.Builder()

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_measuresgroups_dimensions_xsd)
                    for tables in self.executor.get_all_tables_names(
                            ignore_fact=True,):
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
                                '[{0}].[{0}]'.format(tables),)

            return str(xml)

    def mdschema_properties_response(self, request):
        xml = xmlwitch.Builder()
        if request.Restrictions.RestrictionList.PROPERTY_TYPE == 2 \
                and request.Properties.PropertyList.Catalog is not None:
            properties_names = [
                'FONT_FLAGS',
                'LANGUAGE',
                'style',
                'ACTION_TYPE',
                'FONT_SIZE',
                'FORMAT_STRING',
                'className',
                'UPDATEABLE',
                'BACK_COLOR',
                'CELL_ORDINAL',
                'FONT_NAME',
                'VALUE',
                'FORMATTED_VALUE',
                'FORE_COLOR',
            ]
            properties_captions = [
                'FONT_FLAGS',
                'LANGUAGE',
                'style',
                'ACTION_TYPE',
                'FONT_SIZE',
                'FORMAT_STRING',
                'className',
                'UPDATEABLE',
                'BACK_COLOR',
                'CELL_ORDINAL',
                'FONT_NAME',
                'VALUE',
                'FORMATTED_VALUE',
                'FORE_COLOR',
            ]
            properties_datas = [
                '3',
                '19',
                '130',
                '19',
                '18',
                '130',
                '130',
                '19',
                '19',
                '19',
                '130',
                '12',
                '130',
                '19',
            ]

            self.change_catalogue(request.Properties.PropertyList.Catalog)

            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_properties_properties_xsd)
                    for idx, prop_name in enumerate(properties_names):
                        with xml.row:
                            xml.CATALOG_NAME(self.selected_catalogue)
                            xml.PROPERTY_TYPE('2')
                            xml.PROPERTY_NAME(prop_name)
                            xml.PROPERTY_CAPTION(properties_captions[idx])
                            xml.DATA_TYPE(properties_datas[idx])

            return str(xml)

        elif request.Restrictions.RestrictionList.PROPERTY_TYPE == 1:
            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_properties_properties_xsd)

            return str(xml)

    def mdschema_members_response(self, request):
        # Enumeration of hierarchies in all dimensions
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue and \
                request.Properties.PropertyList.Catalog is not None and \
                request.Restrictions.RestrictionList.TREE_OP == 8:
            self.change_catalogue(request.Properties.PropertyList.Catalog)
            separed_tuple = self.executor.parser.split_tuple(
                request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME,)
            joined = ".".join(separed_tuple[:-1])
            # exple
            # separed_tuple -> [Product].[Product].[Company].[Crazy Development]
            # joined -> [Product].[Product].[Company]

            last_attribut = ''.join(
                att for att in separed_tuple[-1] if att not in '[]').replace(
                    '&',
                    '&amp;',
            )
            xml = xmlwitch.Builder()
            with xml['return']:
                with xml.root(
                        xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
                        **{
                            'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema',
                            'xmlns:xsi':
                            'http://www.w3.org/2001/XMLSchema-instance',
                        }):
                    xml.write(mdschema_members_xsd)
                    with xml.row:
                        xml.CATALOG_NAME(self.selected_catalogue)
                        xml.CUBE_NAME(self.selected_catalogue)
                        xml.DIMENSION_UNIQUE_NAME(separed_tuple[0])
                        xml.HIERARCHY_UNIQUE_NAME('{0}.{0}'.format(
                            separed_tuple[0],),)
                        xml.LEVEL_UNIQUE_NAME(joined)
                        xml.LEVEL_NUMBER('0')
                        xml.MEMBER_ORDINAL('0')
                        xml.MEMBER_NAME(last_attribut)
                        xml.MEMBER_UNIQUE_NAME(
                            request.Restrictions.RestrictionList.
                            MEMBER_UNIQUE_NAME,)
                        xml.MEMBER_TYPE('1')
                        xml.MEMBER_CAPTION(last_attribut)
                        xml.CHILDREN_CARDINALITY('1')
                        xml.PARENT_LEVEL('0')
                        xml.PARENT_COUNT('0')
                        xml.MEMBER_KEY(last_attribut)
                        xml.IS_PLACEHOLDERMEMBER('false')
                        xml.IS_DATAMEMBER('false')

            return str(xml)
