from __future__ import absolute_import, division, print_function

from .xmla_discover_xsds import discover_datasources_xsd, discover_preperties_xsd, discover_schema_rowsets_xsd, \
    discover_literals_xsd, mdschema_sets_xsd, mdschema_kpis_xsd, dbschema_catalogs_xsd, mdschema_cubes_xsd, \
    dbschema_tables_xsd, mdschema_measures_xsd, mdschema_dimensions_xsd, mdschema_hierarchies_xsd, mdschema_levels_xsd, \
    mdschema_measuresgroups_xsd, mdschema_measuresgroups_dimensions_xsd, mdschema_properties_PROPERTIES_xsd, \
    mdschema_members_xsd

from lxml import etree
from ..mdx.executor.execute import MdxEngine
import uuid


# TODO clean
class XmlaDiscoverTools():
    """
    XmlaDiscoverTools for generating xmla discover responses
    """
    def __init__(self):
        # right now the catalogue_name and cube name are the same
        self.catalogues = MdxEngine.get_cubes_names()
        self.selected_catalogue = self.catalogues[0]
        self.executer = MdxEngine(self.selected_catalogue)
        self.star_schema_dataframe = self.executer.load_star_schema_dataframe[
            [col for col in self.executer.load_star_schema_dataframe.columns if col[-3:] != "_id"]]
        self.SessionId = uuid.uuid1()

    def change_catalogue(self, new_catalogue):
        """
        if you change the catalogue(cube) in any request, we have to instantiate the MdxEngine with the new catalogue

        :param new_catalogue: catalogue name
        :return: new instance of MdxEngine with new star_schema_DataFrame and other variables
        """
        #
        if self.selected_catalogue != new_catalogue:
            self.selected_catalogue = new_catalogue
            self.executer = MdxEngine(new_catalogue)
            self.star_schema_dataframe = self.executer.load_star_schema_dataframe[
                [col for col in self.executer.load_star_schema_dataframe.columns if col[-3:] != "_id"]]

    def discover_datasources_response(self):
        return etree.fromstring("""
        <return>
            <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
            xmlns:EX="urn:schemas-microsoft-com:xml-analysis:exception"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            """ + discover_datasources_xsd + """
                <row>
                    <DataSourceName>sales</DataSourceName>
                    <DataSourceDescription>sales Sample Data</DataSourceDescription>
                    <URL>http://127.0.0.1:8000/xmla</URL>
                    <DataSourceInfo>-</DataSourceInfo>
                    <ProviderName>olapy</ProviderName>
                    <ProviderType>MDP</ProviderType>
                    <AuthenticationMode>Unauthenticated</AuthenticationMode>
                  </row>
            </root>
        </return>""")

    def discover_properties_response(self, request):
        def get_props(xsd, PropertyName, PropertyDescription, PropertyType, PropertyAccessType, IsRequired, Value):
            return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + xsd + """
                <row>
                    <PropertyName>{0}</PropertyName>
                    <PropertyDescription>{1}</PropertyDescription>
                    <PropertyType>{2}</PropertyType>
                    <PropertyAccessType>{3}</PropertyAccessType>
                    <IsRequired>{4}</IsRequired>
                    <Value>{5}</Value>
                    </row>
                    </root>
            </return>
            """.format(PropertyName, PropertyDescription, PropertyType, PropertyAccessType, IsRequired, Value))

        if request.Restrictions.RestrictionList.PropertyName == 'Catalog':
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                value = self.selected_catalogue
            else:
                value = "olapy Unspecified Catalog"
            return get_props(discover_preperties_xsd, 'Catalog', 'Catalog', 'string', 'ReadWrite', 'false', value)

        elif request.Restrictions.RestrictionList.PropertyName == 'ServerName':
            return get_props(discover_preperties_xsd, 'ServerName', 'ServerName', 'string', 'Read', 'false', 'Mouadh')

        elif request.Restrictions.RestrictionList.PropertyName == 'ProviderVersion':
            return get_props(discover_preperties_xsd, 'ProviderVersion', 'ProviderVersion', 'string', 'Read', 'false',
                             '0.02  08-Mar-2016 08:41:28 GMT')

        elif request.Restrictions.RestrictionList.PropertyName == 'MdpropMdxSubqueries':
            if 'Unspecified' in request.Properties.PropertyList.Catalog:
                return get_props(discover_preperties_xsd, 'MdpropMdxSubqueries', 'MdpropMdxSubqueries', 'int', 'Read',
                                 'false', '15')

            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return get_props(discover_preperties_xsd, 'MdpropMdxSubqueries', 'MdpropMdxSubqueries', 'int', 'Read',
                                 'false', '15')

        elif request.Restrictions.RestrictionList.PropertyName == 'MdpropMdxDrillFunctions':
            if 'Unspecified' in request.Properties.PropertyList.Catalog:
                return get_props(discover_preperties_xsd, 'MdpropMdxDrillFunctions', 'MdpropMdxDrillFunctions', 'int',
                                 'Read',
                                 'false', '3')

            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return get_props(discover_preperties_xsd, 'MdpropMdxDrillFunctions', 'MdpropMdxDrillFunctions', 'int',
                                 'Read',
                                 'false', '3')

        elif request.Restrictions.RestrictionList.PropertyName == 'MdpropMdxNamedSets':
            if 'Unspecified' in request.Properties.PropertyList.Catalog:
                return get_props(discover_preperties_xsd, 'MdpropMdxNamedSets', 'MdpropMdxNamedSets', 'int',
                                 'Read',
                                 'false', '15')

            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return get_props(discover_preperties_xsd, 'MdpropMdxNamedSets', 'MdpropMdxNamedSets', 'int',
                                 'Read',
                                 'false', '15')

        return etree.fromstring("""
        <return>
            <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            """ + discover_preperties_xsd + """
            </root>
        </return>
        """)

    def discover_schema_rowsets_response(self, request):
        if request.Restrictions.RestrictionList.SchemaName == "MDSCHEMA_HIERARCHIES":
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return etree.fromstring("""
                <return>
                    <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    """ + discover_schema_rowsets_xsd + """
                    <row>
                    <SchemaName>MDSCHEMA_HIERARCHIES</SchemaName>
                    <SchemaGuid>C8B522DA-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_ORIGIN</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_VISIBILITY</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>511</RestrictionsMask>
                    </row>
                    </root>
                </return>
            """)
        if request.Restrictions.RestrictionList.SchemaName == 'MDSCHEMA_MEASURES':
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return etree.fromstring("""
                <return>
                    <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                      """ + discover_schema_rowsets_xsd + """
                      <row>
                        <SchemaName>MDSCHEMA_MEASURES</SchemaName>
                        <SchemaGuid>C8B522DC-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                        <Restrictions>
                            <Name>CATALOG_NAME</Name>
                            <Type>string</Type>
                        </Restrictions>
                        <Restrictions>
                            <Name>SCHEMA_NAME</Name>
                            <Type>string</Type>
                        </Restrictions>
                        <Restrictions>
                            <Name>CUBE_NAME</Name>
                            <Type>string</Type>
                        </Restrictions>
                        <Restrictions>
                            <Name>MEASURE_NAME</Name>
                            <Type>string</Type>
                        </Restrictions>
                        <Restrictions>
                            <Name>MEASURE_UNIQUE_NAME</Name>
                            <Type>string</Type>
                        </Restrictions>
                        <Restrictions>
                            <Name>MEASUREGROUP_NAME</Name>
                            <Type>string</Type>
                        </Restrictions>
                        <Restrictions>
                            <Name>CUBE_SOURCE</Name>
                            <Type>unsignedShort</Type>
                        </Restrictions>
                        <Restrictions>
                            <Name>MEASURE_VISIBILITY</Name>
                            <Type>unsignedShort</Type>
                        </Restrictions>
                        <RestrictionsMask>255</RestrictionsMask>
                    </row>
                    </root>
                  </return>
                """)
        # TODO delete
        if request.Properties.PropertyList.Catalog is not None:
            return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                  """ + discover_schema_rowsets_xsd + """
                  <row>
                    <SchemaName>DBSCHEMA_CATALOGS</SchemaName>
                    <SchemaGuid>C8B52211-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>DBSCHEMA_TABLES</SchemaName>
                    <SchemaGuid>C8B52229-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>TABLE_CATALOG</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>TABLE_SCHEMA</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>TABLE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>TABLE_TYPE</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>TABLE_OLAP_TYPE</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>DISCOVER_DATASOURCES</SchemaName>
                    <SchemaGuid>06C03D41-F66D-49F3-B1B8-987F7AF4CF18</SchemaGuid>
                    <Restrictions>
                      <Name>DataSourceName</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>URL</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>ProviderName</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>ProviderType</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>AuthenticationMode</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>DISCOVER_INSTANCES</SchemaName>
                    <SchemaGuid>20518699-2474-4C15-9885-0E947EC7A7E3</SchemaGuid>
                    <Restrictions>
                      <Name>INSTANCE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>DISCOVER_KEYWORDS</SchemaName>
                    <SchemaGuid>1426C443-4CDD-4A40-8F45-572FAB9BBAA1</SchemaGuid>
                    <Restrictions>
                      <Name>Keyword</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>DISCOVER_LITERALS</SchemaName>
                    <SchemaGuid>C3EF5ECB-0A07-4665-A140-B075722DBDC2</SchemaGuid>
                    <Restrictions>
                      <Name>LiteralName</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>DISCOVER_PROPERTIES</SchemaName>
                    <SchemaGuid>4B40ADFB-8B09-4758-97BB-636E8AE97BCF</SchemaGuid>
                    <Restrictions>
                      <Name>PropertyName</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>DISCOVER_SCHEMA_ROWSETS</SchemaName>
                    <SchemaGuid>EEA0302B-7922-4992-8991-0E605D0E5593</SchemaGuid>
                    <Restrictions>
                      <Name>SchemaName</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>DMSCHEMA_MINING_MODELS</SchemaName>
                    <SchemaGuid>3ADD8A77-D8B9-11D2-8D2A-00E029154FDE</SchemaGuid>
                    <Restrictions>
                      <Name>MODEL_CATALOG</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MODEL_SCHEMA</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MODEL_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MODEL_TYPE</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SERVICE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SERVICE_TYPE_ID</Name>
                      <Type>unsignedInt</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MINING_STRUCTURE</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>127</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_ACTIONS</SchemaName>
                    <SchemaGuid>A07CCD08-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>ACTION_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>ACTION_TYPE</Name>
                      <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>COORDINATE</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>COORDINATE_TYPE</Name>
                      <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>INVOCATION</Name>
                      <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>511</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_CUBES</SchemaName>
                    <SchemaGuid>C8B522D8-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>BASE_CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_DIMENSIONS</SchemaName>
                    <SchemaGuid>C8B522D9-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_VISIBILITY</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>127</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_FUNCTIONS</SchemaName>
                    <SchemaGuid>A07CCD07-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                      <Name>LIBRARY_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>INTERFACE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>FUNCTION_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>ORIGIN</Name>
                      <Type>int</Type>
                    </Restrictions>
                    <RestrictionsMask>15</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_HIERARCHIES</SchemaName>
                    <SchemaGuid>C8B522DA-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_ORIGIN</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_VISIBILITY</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>511</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_INPUT_DATASOURCES</SchemaName>
                    <SchemaGuid>A07CCD32-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DATASOURCE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DATASOURCE_TYPE</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>15</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_KPIS</SchemaName>
                    <SchemaGuid>2AE44109-ED3D-4842-B16F-B694D1CB0E3F</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>KPI_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_LEVELS</SchemaName>
                    <SchemaGuid>C8B522DB-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>LEVEL_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>LEVEL_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>LEVEL_ORIGIN</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>LEVEL_VISIBILITY</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>1023</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_MEASUREGROUPS</SchemaName>
                    <SchemaGuid>E1625EBF-FA96-42FD-BEA6-DB90ADAFD96B</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEASUREGROUP_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>15</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_MEASUREGROUP_DIMENSIONS</SchemaName>
                    <SchemaGuid>A07CCD33-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEASUREGROUP_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_VISIBILITY</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>63</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_MEASURES</SchemaName>
                    <SchemaGuid>C8B522DC-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEASURE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEASURE_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEASUREGROUP_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEASURE_VISIBILITY</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>255</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_MEMBERS</SchemaName>
                    <SchemaGuid>C8B522DE-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>LEVEL_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>LEVEL_NUMBER</Name>
                      <Type>unsignedInt</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEMBER_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEMBER_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEMBER_CAPTION</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEMBER_TYPE</Name>
                      <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>TREE_OP</Name>
                      <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>8191</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_PROPERTIES</SchemaName>
                    <SchemaGuid>C8B522DD-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>DIMENSION_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>HIERARCHY_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>LEVEL_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>MEMBER_UNIQUE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>PROPERTY_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>PROPERTY_TYPE</Name>
                      <Type>short</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>PROPERTY_CONTENT_TYPE</Name>
                      <Type>short</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>PROPERTY_ORIGIN</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_SOURCE</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>PROPERTY_VISIBILITY</Name>
                      <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>8191</RestrictionsMask>
                  </row>
                  <row>
                    <SchemaName>MDSCHEMA_SETS</SchemaName>
                    <SchemaGuid>A07CCD0B-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                      <Name>CATALOG_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCHEMA_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>CUBE_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SET_NAME</Name>
                      <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                      <Name>SCOPE</Name>
                      <Type>int</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                  </row>
                </root>
              </return>
            """)

        return etree.fromstring("""
            <return>
            <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + discover_schema_rowsets_xsd + """
                <row>
                    <SchemaName>DBSCHEMA_CATALOGS</SchemaName>
                    <SchemaGuid>C8B52211-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>DBSCHEMA_TABLES</SchemaName>
                    <SchemaGuid>C8B52229-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>TABLE_CATALOG</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>TABLE_SCHEMA</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>TABLE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>TABLE_TYPE</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>TABLE_OLAP_TYPE</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>DISCOVER_DATASOURCES</SchemaName>
                    <SchemaGuid>06C03D41-F66D-49F3-B1B8-987F7AF4CF18</SchemaGuid>
                    <Restrictions>
                        <Name>DataSourceName</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>URL</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>ProviderName</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>ProviderType</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>AuthenticationMode</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>DISCOVER_INSTANCES</SchemaName>
                    <SchemaGuid>20518699-2474-4C15-9885-0E947EC7A7E3</SchemaGuid>
                    <Restrictions>
                        <Name>INSTANCE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>DISCOVER_KEYWORDS</SchemaName>
                    <SchemaGuid>1426C443-4CDD-4A40-8F45-572FAB9BBAA1</SchemaGuid>
                    <Restrictions>
                        <Name>Keyword</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>DISCOVER_LITERALS</SchemaName>
                    <SchemaGuid>C3EF5ECB-0A07-4665-A140-B075722DBDC2</SchemaGuid>
                    <Restrictions>
                        <Name>LiteralName</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>DISCOVER_PROPERTIES</SchemaName>
                    <SchemaGuid>4B40ADFB-8B09-4758-97BB-636E8AE97BCF</SchemaGuid>
                    <Restrictions>
                        <Name>PropertyName</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>DISCOVER_SCHEMA_ROWSETS</SchemaName>
                    <SchemaGuid>EEA0302B-7922-4992-8991-0E605D0E5593</SchemaGuid>
                    <Restrictions>
                        <Name>SchemaName</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>1</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>DMSCHEMA_MINING_MODELS</SchemaName>
                    <SchemaGuid>3ADD8A77-D8B9-11D2-8D2A-00E029154FDE</SchemaGuid>
                    <Restrictions>
                        <Name>MODEL_CATALOG</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MODEL_SCHEMA</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MODEL_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MODEL_TYPE</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SERVICE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SERVICE_TYPE_ID</Name>
                        <Type>unsignedInt</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MINING_STRUCTURE</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>127</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_ACTIONS</SchemaName>
                    <SchemaGuid>A07CCD08-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>ACTION_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>ACTION_TYPE</Name>
                        <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>COORDINATE</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>COORDINATE_TYPE</Name>
                        <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>INVOCATION</Name>
                        <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>511</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_CUBES</SchemaName>
                    <SchemaGuid>C8B522D8-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>BASE_CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_DIMENSIONS</SchemaName>
                    <SchemaGuid>C8B522D9-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_VISIBILITY</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>127</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_FUNCTIONS</SchemaName>
                    <SchemaGuid>A07CCD07-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                        <Name>LIBRARY_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>INTERFACE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>FUNCTION_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>ORIGIN</Name>
                        <Type>int</Type>
                    </Restrictions>
                    <RestrictionsMask>15</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_HIERARCHIES</SchemaName>
                    <SchemaGuid>C8B522DA-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>HIERARCHY_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>HIERARCHY_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>HIERARCHY_ORIGIN</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>HIERARCHY_VISIBILITY</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>511</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_INPUT_DATASOURCES</SchemaName>
                    <SchemaGuid>A07CCD32-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DATASOURCE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DATASOURCE_TYPE</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>15</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_KPIS</SchemaName>
                    <SchemaGuid>2AE44109-ED3D-4842-B16F-B694D1CB0E3F</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>KPI_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_LEVELS</SchemaName>
                    <SchemaGuid>C8B522DB-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>HIERARCHY_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>LEVEL_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>LEVEL_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>LEVEL_ORIGIN</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>LEVEL_VISIBILITY</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>1023</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_MEASUREGROUPS</SchemaName>
                    <SchemaGuid>E1625EBF-FA96-42FD-BEA6-DB90ADAFD96B</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEASUREGROUP_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <RestrictionsMask>15</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_MEASUREGROUP_DIMENSIONS</SchemaName>
                    <SchemaGuid>A07CCD33-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEASUREGROUP_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_VISIBILITY</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>63</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_MEASURES</SchemaName>
                    <SchemaGuid>C8B522DC-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEASURE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEASURE_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEASUREGROUP_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEASURE_VISIBILITY</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>255</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_MEMBERS</SchemaName>
                    <SchemaGuid>C8B522DE-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>HIERARCHY_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>LEVEL_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>LEVEL_NUMBER</Name>
                        <Type>unsignedInt</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEMBER_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEMBER_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEMBER_CAPTION</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEMBER_TYPE</Name>
                        <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>TREE_OP</Name>
                        <Type>int</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>8191</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_PROPERTIES</SchemaName>
                    <SchemaGuid>C8B522DD-5CF3-11CE-ADE5-00AA0044773D</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>DIMENSION_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>HIERARCHY_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>LEVEL_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>MEMBER_UNIQUE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>PROPERTY_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>PROPERTY_TYPE</Name>
                        <Type>short</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>PROPERTY_CONTENT_TYPE</Name>
                        <Type>short</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>PROPERTY_ORIGIN</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_SOURCE</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>PROPERTY_VISIBILITY</Name>
                        <Type>unsignedShort</Type>
                    </Restrictions>
                    <RestrictionsMask>8191</RestrictionsMask>
                </row>
                <row>
                    <SchemaName>MDSCHEMA_SETS</SchemaName>
                    <SchemaGuid>A07CCD0B-8148-11D0-87BB-00C04FC33942</SchemaGuid>
                    <Restrictions>
                        <Name>CATALOG_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCHEMA_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>CUBE_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SET_NAME</Name>
                        <Type>string</Type>
                    </Restrictions>
                    <Restrictions>
                        <Name>SCOPE</Name>
                        <Type>int</Type>
                    </Restrictions>
                    <RestrictionsMask>31</RestrictionsMask>
                </row>
            </root>
        </return>
            """)

    def discover_literals_response(self, request):
        if request.Properties.PropertyList.Content == 'SchemaData':
            if request.Properties.PropertyList.Format == 'Tabular':
                return etree.fromstring("""
                <return>
            <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                """ + discover_literals_xsd + """
              <row>
                <LiteralName>DBLITERAL_CATALOG_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>24</LiteralMaxLength>
                <LiteralNameEnumValue>2</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_CATALOG_SEPARATOR</LiteralName>
                <LiteralValue>.</LiteralValue>
                <LiteralMaxLength>0</LiteralMaxLength>
                <LiteralNameEnumValue>3</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_COLUMN_ALIAS</LiteralName>
                <LiteralInvalidChars>'&quot;[]</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>5</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_COLUMN_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>6</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_CORRELATION_NAME</LiteralName>
                <LiteralInvalidChars>'&quot;[]</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>7</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_CUBE_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>21</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_DIMENSION_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>22</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_HIERARCHY_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>23</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_LEVEL_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>24</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_MEMBER_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>25</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_PROCEDURE_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>14</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_PROPERTY_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>26</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_QUOTE_PREFIX</LiteralName>
                <LiteralValue>[</LiteralValue>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>15</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_QUOTE_SUFFIX</LiteralName>
                <LiteralValue>]</LiteralValue>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>28</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_TABLE_NAME</LiteralName>
                <LiteralInvalidChars>.</LiteralInvalidChars>
                <LiteralInvalidStartingChars>0123456789</LiteralInvalidStartingChars>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>17</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_TEXT_COMMAND</LiteralName>
                <LiteralMaxLength>-1</LiteralMaxLength>
                <LiteralNameEnumValue>18</LiteralNameEnumValue>
              </row>
              <row>
                <LiteralName>DBLITERAL_USER_NAME</LiteralName>
                <LiteralMaxLength>0</LiteralMaxLength>
                <LiteralNameEnumValue>19</LiteralNameEnumValue>
              </row>
            </root>
          </return>
                """)

    def discover_mdschema_sets_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return etree.fromstring("""
                <return>
                    <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                    """ + mdschema_sets_xsd + """
                    </root>
                  </return>
                """)

    def discover_mdschema_kpis_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                return etree.fromstring("""
            <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                  """ + mdschema_kpis_xsd + """
                </root>
              </return>
                """)

    def discover_dbschema_catalogs_response(self, request):
        _catalogues = ''
        for cata in self.catalogues:
            _catalogues += '''
            <row>
                <CATALOG_NAME>{0}</CATALOG_NAME>
            </row>
            '''.format(cata)

        return etree.fromstring("""
        <return>
            <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            """ + dbschema_catalogs_xsd + """
            {0}
            </root>
        </return>
        """.format(_catalogues))

    def discover_mdschema_cubes_response(self, request):
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Properties.PropertyList.Catalog is not None:
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
                          """.format(self.selected_catalogue, self.selected_catalogue))
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
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Restrictions.RestrictionList.MEASURE_VISIBILITY == 3:
                if request.Properties.PropertyList.Catalog is not None:
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
                        """.format(self.selected_catalogue,
                                   self.selected_catalogue, mes)
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
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue:
                if request.Properties.PropertyList.Catalog is not None:
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
                        </row>""".format(self.selected_catalogue,
                                         tables,
                                         ord)
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
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                if request.Restrictions.RestrictionList.HIERARCHY_VISIBILITY == 3:
                    rows = ""
                    for table_name, df in self.executer.tables_loaded.items():
                        # column = XmlaProvider.executer.get_columns_to_index(tables, 1, 0)[0]
                        if table_name == self.executer.facts:
                            continue
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
                        """.format(
                            self.selected_catalogue, table_name, df.columns[0],
                            df.iloc[0][0])
                        # self.executer.get_attribute_column_rm_id(tables, column, 0))

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
                            """.format(self.selected_catalogue,
                                       table_name,
                                       df.columns[0],
                                       df.iloc[0][0])

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
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Restrictions.RestrictionList.CATALOG_NAME == self.selected_catalogue:
                if request.Properties.PropertyList.Catalog is not None:
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
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Properties.PropertyList.Catalog is not None:
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
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Properties.PropertyList.Catalog is not None:
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
        if request.Restrictions.RestrictionList.PROPERTY_TYPE == 2:
            if request.Properties.PropertyList.Catalog is not None:
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
                """.format(self.selected_catalogue, mdschema_properties_PROPERTIES_xsd))
        elif request.Restrictions.RestrictionList.PROPERTY_TYPE == 1:
            return etree.fromstring("""
              <return>
                <root xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                 {0}
                </root>
              </return>
            """.format(mdschema_properties_PROPERTIES_xsd))

    def discover_mdschema_members_response(self, request):
        # Enumeration of hierarchies in all dimensions
        if request.Restrictions.RestrictionList.CUBE_NAME == self.selected_catalogue:
            if request.Properties.PropertyList.Catalog is not None:
                self.change_catalogue(request.Properties.PropertyList.Catalog)
                if request.Restrictions.RestrictionList.TREE_OP == 8:
                    separed_tuple = request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME.split(".")
                    joined = ".".join(separed_tuple[:-1])
                    # exple
                    # separed_tuple -> [Product].[Product].[Company].[Crazy Development]
                    # joined -> [Product].[Product].[Company]

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
                                    <MEMBER_NAME>{4}</MEMBER_NAME>
                                    <MEMBER_UNIQUE_NAME>{3}</MEMBER_UNIQUE_NAME>
                                    <MEMBER_TYPE>1</MEMBER_TYPE>
                                    <MEMBER_CAPTION>{4}</MEMBER_CAPTION>
                                    <CHILDREN_CARDINALITY>1</CHILDREN_CARDINALITY>
                                    <PARENT_LEVEL>0</PARENT_LEVEL>
                                    <PARENT_COUNT>0</PARENT_COUNT>
                                    <MEMBER_KEY>{4}</MEMBER_KEY>
                                    <IS_PLACEHOLDERMEMBER>false</IS_PLACEHOLDERMEMBER>
                                    <IS_DATAMEMBER>false</IS_DATAMEMBER>
                                </row>
                            </root>
                        </return>
                            """.format(self.selected_catalogue, separed_tuple[
                        0], joined, request.Restrictions.RestrictionList.MEMBER_UNIQUE_NAME, ''.join(
                        c for c in separed_tuple[-1] if c not in '[]')))
