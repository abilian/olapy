# TODO clean (or fusion)
discover_datasources_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql"
xmlns:xsd="http://www.w3.org/2001/XMLSchema"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence>
                <xsd:element maxOccurs="unbounded" minOccurs="0" name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="xsd:string">
            <xsd:pattern value="[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element name="PropertyName" sql:field="PropertyName" type="string"/>
            <xsd:element minOccurs="0" name="PropertyDescription" sql:field="PropertyDescription" type="string"/>
            <xsd:element name="DataSourceName" sql:field="DataSourceName" type="xs:string"/>
            <xsd:element minOccurs="0" name="DataSourceDescription" sql:field="DataSourceDescription" type="string"/>
            <xsd:element minOccurs="0" name="URL" sql:field="URL" type="string"/>
            <xsd:element minOccurs="0" name="DataSourceInfo" sql:field="DataSourceInfo" type="string"/>
            <xsd:element minOccurs="0" name="ProviderName" sql:field="ProviderName" type="string"/>
            <xsd:element maxOccurs="unbounded" name="ProviderType" sql:field="ProviderType" type="string"/>
            <xsd:element name="AuthenticationMode" sql:field="AuthenticationMode" type="string"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

discover_preperties_xsd = """
<xsd:schema elementFormDefault="qualified"
    targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
    xmlns:sql="urn:schemas-microsoft-com:xml-sql">
        <xsd:element name="root">
            <xsd:complexType>
                <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                    <xsd:element name="row" type="row"/>
                </xsd:sequence>
            </xsd:complexType>
        </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element name="PropertyName" sql:field="PropertyName" type="string"/>
            <xsd:element minOccurs="0" name="PropertyDescription" sql:field="PropertyDescription" type="string"/>
            <xsd:element minOccurs="0" name="PropertyType" sql:field="PropertyType" type="string"/>
            <xsd:element name="PropertyAccessType" sql:field="PropertyAccessType" type="string"/>
            <xsd:element minOccurs="0" name="IsRequired" sql:field="IsRequired" type="boolean"/>
            <xsd:element minOccurs="0" name="Value" sql:field="Value" type="string"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

discover_schema_rowsets_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
    <xsd:complexType>
        <xsd:sequence maxOccurs="unbounded" minOccurs="0">
            <xsd:element name="row" type="row"/>
        </xsd:sequence>
    </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="SchemaName" sql:field="SchemaName" type="string"/>
            <xsd:element minOccurs="0" name="SchemaGuid" sql:field="SchemaGuid" type="uuid"/>
            <xsd:element maxOccurs="unbounded" minOccurs="0" name="Restrictions" sql:field="Restrictions">
        <xsd:complexType>
        <xsd:sequence>
            <xsd:element minOccurs="0" name="Name" sql:field="Name" type="string"/>
            <xsd:element minOccurs="0" name="Type" sql:field="Type" type="string"/>
        </xsd:sequence>
    </xsd:complexType>
    </xsd:element>
    <xsd:element minOccurs="0" name="Description" sql:field="Description" type="string"/>
        <xsd:element minOccurs="0" name="RestrictionsMask" sql:field="RestrictionsMask" type="unsignedLong"/>
    </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

discover_literals_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="LiteralName" sql:field="LiteralName" type="string"/>
            <xsd:element minOccurs="0" name="LiteralValue" sql:field="LiteralValue" type="string"/>
            <xsd:element minOccurs="0" name="LiteralInvalidChars" sql:field="LiteralInvalidChars" type="string"/>
            <xsd:element minOccurs="0" name="LiteralInvalidStartingChars" sql:field="LiteralInvalidStartingChars" type="string"/>
            <xsd:element minOccurs="0" name="LiteralMaxLength" sql:field="LiteralMaxLength" type="int"/>
            <xsd:element minOccurs="0" name="LiteralNameEnumValue" sql:field="LiteralNameEnumValue" type="int"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_sets_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SET_NAME" sql:field="SET_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCOPE" sql:field="SCOPE" type="int"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="EXPRESSION" sql:field="EXPRESSION" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSIONS" sql:field="DIMENSIONS" type="string"/>
            <xsd:element minOccurs="0" name="SET_CAPTION" sql:field="SET_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="SET_DISPLAY_FOLDER" sql:field="SET_DISPLAY_FOLDER" type="string"/>
            <xsd:element minOccurs="0" name="SET_EVALUATION_CONTEXT" sql:field="SET_EVALUATION_CONTEXT" type="int"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_kpis_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASUREGROUP_NAME" sql:field="MEASUREGROUP_NAME" type="string"/>
            <xsd:element minOccurs="0" name="KPI_NAME" sql:field="KPI_NAME" type="string"/>
            <xsd:element minOccurs="0" name="KPI_CAPTION" sql:field="KPI_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="KPI_DESCRIPTION" sql:field="KPI_DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="KPI_DISPLAY_FOLDER" sql:field="KPI_DISPLAY_FOLDER" type="string"/>
            <xsd:element minOccurs="0" name="KPI_VALUE" sql:field="KPI_VALUE" type="string"/>
            <xsd:element minOccurs="0" name="KPI_GOAL" sql:field="KPI_GOAL" type="string"/>
            <xsd:element minOccurs="0" name="KPI_STATUS" sql:field="KPI_STATUS" type="string"/>
            <xsd:element minOccurs="0" name="KPI_TREND" sql:field="KPI_TREND" type="string"/>
            <xsd:element minOccurs="0" name="KPI_STATUS_GRAPHIC" sql:field="KPI_STATUS_GRAPHIC" type="string"/>
            <xsd:element minOccurs="0" name="KPI_TREND_GRAPHIC" sql:field="KPI_TREND_GRAPHIC" type="string"/>
            <xsd:element minOccurs="0" name="KPI_WEIGHT" sql:field="KPI_WEIGHT" type="string"/>
            <xsd:element minOccurs="0" name="KPI_CURRENT_TIME_MEMBER" sql:field="KPI_CURRENT_TIME_MEMBER" type="string"/>
            <xsd:element minOccurs="0" name="KPI_PARENT_KPI_NAME" sql:field="KPI_PARENT_KPI_NAME" type="string"/>
            <xsd:element minOccurs="0" name="ANNOTATIONS" sql:field="ANNOTATIONS" type="string"/>
            <xsd:element minOccurs="0" name="SCOPE" sql:field="SCOPE" type="int"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

dbschema_catalogs_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
        <xsd:complexType name="xmlDocument">
            <xsd:sequence>
                <xsd:any/>
            </xsd:sequence>
        </xsd:complexType>
        <xsd:complexType name="row">
            <xsd:sequence>
                <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
                <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
                <xsd:element minOccurs="0" name="ROLES" sql:field="ROLES" type="string"/>
                <xsd:element minOccurs="0" name="DATE_MODIFIED" sql:field="DATE_MODIFIED" type="dateTime"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:schema>
"""

mdschema_cubes_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_TYPE" sql:field="CUBE_TYPE" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_GUID" sql:field="CUBE_GUID" type="uuid"/>
            <xsd:element minOccurs="0" name="CREATED_ON" sql:field="CREATED_ON" type="dateTime"/>
            <xsd:element minOccurs="0" name="LAST_SCHEMA_UPDATE" sql:field="LAST_SCHEMA_UPDATE" type="dateTime"/>
            <xsd:element minOccurs="0" name="SCHEMA_UPDATED_BY" sql:field="SCHEMA_UPDATED_BY" type="string"/>
            <xsd:element minOccurs="0" name="LAST_DATA_UPDATE" sql:field="LAST_DATA_UPDATE" type="dateTime"/>
            <xsd:element minOccurs="0" name="DATA_UPDATED_BY" sql:field="DATA_UPDATED_BY" type="string"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="IS_DRILLTHROUGH_ENABLED" sql:field="IS_DRILLTHROUGH_ENABLED" type="boolean"/>
            <xsd:element minOccurs="0" name="IS_LINKABLE" sql:field="IS_LINKABLE" type="boolean"/>
            <xsd:element minOccurs="0" name="IS_WRITE_ENABLED" sql:field="IS_WRITE_ENABLED" type="boolean"/>
            <xsd:element minOccurs="0" name="IS_SQL_ENABLED" sql:field="IS_SQL_ENABLED" type="boolean"/>
            <xsd:element minOccurs="0" name="CUBE_CAPTION" sql:field="CUBE_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="BASE_CUBE_NAME" sql:field="BASE_CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_SOURCE" sql:field="CUBE_SOURCE" type="unsignedShort"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

dbschema_tables_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="TABLE_CATALOG" sql:field="TABLE_CATALOG" type="string"/>
            <xsd:element minOccurs="0" name="TABLE_SCHEMA" sql:field="TABLE_SCHEMA" type="string"/>
            <xsd:element minOccurs="0" name="TABLE_NAME" sql:field="TABLE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="TABLE_TYPE" sql:field="TABLE_TYPE" type="string"/>
            <xsd:element minOccurs="0" name="TABLE_GUID" sql:field="TABLE_GUID" type="uuid"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="TABLE_PROPID" sql:field="TABLE_PROPID" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="DATE_CREATED" sql:field="DATE_CREATED" type="dateTime"/>
            <xsd:element minOccurs="0" name="DATE_MODIFIED" sql:field="DATE_MODIFIED" type="dateTime"/>
            <xsd:element minOccurs="0" name="TABLE_OLAP_TYPE" sql:field="TABLE_OLAP_TYPE" type="string"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_measures_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASURE_NAME" sql:field="MEASURE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASURE_UNIQUE_NAME" sql:field="MEASURE_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASURE_CAPTION" sql:field="MEASURE_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="MEASURE_GUID" sql:field="MEASURE_GUID" type="uuid"/>
            <xsd:element minOccurs="0" name="MEASURE_AGGREGATOR" sql:field="MEASURE_AGGREGATOR" type="int"/>
            <xsd:element minOccurs="0" name="DATA_TYPE" sql:field="DATA_TYPE" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="NUMERIC_PRECISION" sql:field="NUMERIC_PRECISION" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="NUMERIC_SCALE" sql:field="NUMERIC_SCALE" type="short"/>
            <xsd:element minOccurs="0" name="MEASURE_UNITS" sql:field="MEASURE_UNITS" type="string"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="EXPRESSION" sql:field="EXPRESSION" type="string"/>
            <xsd:element minOccurs="0" name="MEASURE_IS_VISIBLE" sql:field="MEASURE_IS_VISIBLE" type="boolean"/>
            <xsd:element minOccurs="0" name="LEVELS_LIST" sql:field="LEVELS_LIST" type="string"/>
            <xsd:element minOccurs="0" name="MEASURE_NAME_SQL_COLUMN_NAME" sql:field="MEASURE_NAME_SQL_COLUMN_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASURE_UNQUALIFIED_CAPTION" sql:field="MEASURE_UNQUALIFIED_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="MEASUREGROUP_NAME" sql:field="MEASUREGROUP_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASURE_DISPLAY_FOLDER" sql:field="MEASURE_DISPLAY_FOLDER" type="string"/>
            <xsd:element minOccurs="0" name="DEFAULT_FORMAT_STRING" sql:field="DEFAULT_FORMAT_STRING" type="string"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_dimensions_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_NAME" sql:field="DIMENSION_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_UNIQUE_NAME" sql:field="DIMENSION_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_GUID" sql:field="DIMENSION_GUID" type="uuid"/>
            <xsd:element minOccurs="0" name="DIMENSION_CAPTION" sql:field="DIMENSION_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_ORDINAL" sql:field="DIMENSION_ORDINAL" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="DIMENSION_TYPE" sql:field="DIMENSION_TYPE" type="short"/>
            <xsd:element minOccurs="0" name="DIMENSION_CARDINALITY" sql:field="DIMENSION_CARDINALITY" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="DEFAULT_HIERARCHY" sql:field="DEFAULT_HIERARCHY" type="string"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="IS_VIRTUAL" sql:field="IS_VIRTUAL" type="boolean"/>
            <xsd:element minOccurs="0" name="IS_READWRITE" sql:field="IS_READWRITE" type="boolean"/>
            <xsd:element minOccurs="0" name="DIMENSION_UNIQUE_SETTINGS" sql:field="DIMENSION_UNIQUE_SETTINGS" type="int"/>
            <xsd:element minOccurs="0" name="DIMENSION_MASTER_NAME" sql:field="DIMENSION_MASTER_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_IS_VISIBLE" sql:field="DIMENSION_IS_VISIBLE" type="boolean"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_hierarchies_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_UNIQUE_NAME" sql:field="DIMENSION_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="HIERARCHY_NAME" sql:field="HIERARCHY_NAME" type="string"/>
            <xsd:element minOccurs="0" name="HIERARCHY_UNIQUE_NAME" sql:field="HIERARCHY_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="HIERARCHY_GUID" sql:field="HIERARCHY_GUID" type="uuid"/>
            <xsd:element minOccurs="0" name="HIERARCHY_CAPTION" sql:field="HIERARCHY_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_TYPE" sql:field="DIMENSION_TYPE" type="short"/>
            <xsd:element minOccurs="0" name="HIERARCHY_CARDINALITY" sql:field="HIERARCHY_CARDINALITY" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="DEFAULT_MEMBER" sql:field="DEFAULT_MEMBER" type="string"/>
            <xsd:element minOccurs="0" name="ALL_MEMBER" sql:field="ALL_MEMBER" type="string"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="STRUCTURE" sql:field="STRUCTURE" type="short"/>
            <xsd:element minOccurs="0" name="IS_VIRTUAL" sql:field="IS_VIRTUAL" type="boolean"/>
            <xsd:element minOccurs="0" name="IS_READWRITE" sql:field="IS_READWRITE" type="boolean"/>
            <xsd:element minOccurs="0" name="DIMENSION_UNIQUE_SETTINGS" sql:field="DIMENSION_UNIQUE_SETTINGS" type="int"/>
            <xsd:element minOccurs="0" name="DIMENSION_MASTER_UNIQUE_NAME" sql:field="DIMENSION_MASTER_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_IS_VISIBLE" sql:field="DIMENSION_IS_VISIBLE" type="boolean"/>
            <xsd:element minOccurs="0" name="HIERARCHY_ORDINAL" sql:field="HIERARCHY_ORDINAL" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="DIMENSION_IS_SHARED" sql:field="DIMENSION_IS_SHARED" type="boolean"/>
            <xsd:element minOccurs="0" name="HIERARCHY_IS_VISIBLE" sql:field="HIERARCHY_IS_VISIBLE" type="boolean"/>
            <xsd:element minOccurs="0" name="HIERARCHY_ORIGIN" sql:field="HIERARCHY_ORIGIN" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="HIERARCHY_DISPLAY_FOLDER" sql:field="HIERARCHY_DISPLAY_FOLDER" type="string"/>
            <xsd:element minOccurs="0" name="INSTANCE_SELECTION" sql:field="INSTANCE_SELECTION" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="GROUPING_BEHAVIOR" sql:field="GROUPING_BEHAVIOR" type="unsignedShort"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_levels_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
        <xsd:simpleType name="uuid">
            <xsd:restriction base="string">
                <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
            </xsd:restriction>
        </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_UNIQUE_NAME" sql:field="DIMENSION_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="HIERARCHY_UNIQUE_NAME" sql:field="HIERARCHY_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_NAME" sql:field="LEVEL_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_UNIQUE_NAME" sql:field="LEVEL_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_GUID" sql:field="LEVEL_GUID" type="uuid"/>
            <xsd:element minOccurs="0" name="LEVEL_CAPTION" sql:field="LEVEL_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_NUMBER" sql:field="LEVEL_NUMBER" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="LEVEL_CARDINALITY" sql:field="LEVEL_CARDINALITY" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="LEVEL_TYPE" sql:field="LEVEL_TYPE" type="int"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="CUSTOM_ROLLUP_SETTINGS" sql:field="CUSTOM_ROLLUP_SETTINGS" type="int"/>
            <xsd:element minOccurs="0" name="LEVEL_UNIQUE_SETTINGS" sql:field="LEVEL_UNIQUE_SETTINGS" type="int"/>
            <xsd:element minOccurs="0" name="LEVEL_IS_VISIBLE" sql:field="LEVEL_IS_VISIBLE" type="boolean"/>
            <xsd:element minOccurs="0" name="LEVEL_ORDERING_PROPERTY" sql:field="LEVEL_ORDERING_PROPERTY" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_DBTYPE" sql:field="LEVEL_DBTYPE" type="int"/>
            <xsd:element minOccurs="0" name="LEVEL_MASTER_UNIQUE_NAME" sql:field="LEVEL_MASTER_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_NAME_SQL_COLUMN_NAME" sql:field="LEVEL_NAME_SQL_COLUMN_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_KEY_SQL_COLUMN_NAME" sql:field="LEVEL_KEY_SQL_COLUMN_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_UNIQUE_NAME_SQL_COLUMN_NAME" sql:field="LEVEL_UNIQUE_NAME_SQL_COLUMN_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_ATTRIBUTE_HIERARCHY_NAME" sql:field="LEVEL_ATTRIBUTE_HIERARCHY_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_KEY_CARDINALITY" sql:field="LEVEL_KEY_CARDINALITY" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="LEVEL_ORIGIN" sql:field="LEVEL_ORIGIN" type="unsignedShort"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_measuresgroups_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASUREGROUP_NAME" sql:field="MEASUREGROUP_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="IS_WRITE_ENABLED" sql:field="IS_WRITE_ENABLED" type="boolean"/>
            <xsd:element minOccurs="0" name="MEASUREGROUP_CAPTION" sql:field="MEASUREGROUP_CAPTION" type="string"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_measuresgroups_dimensions_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASUREGROUP_NAME" sql:field="MEASUREGROUP_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEASUREGROUP_CARDINALITY" sql:field="MEASUREGROUP_CARDINALITY" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_UNIQUE_NAME" sql:field="DIMENSION_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_CARDINALITY" sql:field="DIMENSION_CARDINALITY" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_IS_VISIBLE" sql:field="DIMENSION_IS_VISIBLE" type="boolean"/>
            <xsd:element minOccurs="0" name="DIMENSION_IS_FACT_DIMENSION" sql:field="DIMENSION_IS_FACT_DIMENSION" type="boolean"/>
            <xsd:element maxOccurs="unbounded" minOccurs="0" name="DIMENSION_PATH" sql:field="DIMENSION_PATH">
            <xsd:complexType>
                <xsd:sequence>
                    <xsd:element minOccurs="0" name="MeasureGroupDimension" sql:field="MeasureGroupDimension" type="string"/>
                </xsd:sequence>
            </xsd:complexType>
            </xsd:element>
            <xsd:element minOccurs="0" name="DIMENSION_GRANULARITY" sql:field="DIMENSION_GRANULARITY" type="string"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_properties_PROPERTIES_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_UNIQUE_NAME" sql:field="DIMENSION_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="HIERARCHY_UNIQUE_NAME" sql:field="HIERARCHY_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_UNIQUE_NAME" sql:field="LEVEL_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEMBER_UNIQUE_NAME" sql:field="MEMBER_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="PROPERTY_TYPE" sql:field="PROPERTY_TYPE" type="short"/>
            <xsd:element minOccurs="0" name="PROPERTY_NAME" sql:field="PROPERTY_NAME" type="string"/>
            <xsd:element minOccurs="0" name="PROPERTY_CAPTION" sql:field="PROPERTY_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="DATA_TYPE" sql:field="DATA_TYPE" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="CHARACTER_MAXIMUM_LENGTH" sql:field="CHARACTER_MAXIMUM_LENGTH" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="CHARACTER_OCTET_LENGTH" sql:field="CHARACTER_OCTET_LENGTH" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="NUMERIC_PRECISION" sql:field="NUMERIC_PRECISION" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="NUMERIC_SCALE" sql:field="NUMERIC_SCALE" type="short"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="PROPERTY_CONTENT_TYPE" sql:field="PROPERTY_CONTENT_TYPE" type="short"/>
            <xsd:element minOccurs="0" name="SQL_COLUMN_NAME" sql:field="SQL_COLUMN_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LANGUAGE" sql:field="LANGUAGE" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="PROPERTY_ORIGIN" sql:field="PROPERTY_ORIGIN" type="unsignedShort"/>
            <xsd:element minOccurs="0" name="PROPERTY_ATTRIBUTE_HIERARCHY_NAME" sql:field="PROPERTY_ATTRIBUTE_HIERARCHY_NAME" type="string"/>
            <xsd:element minOccurs="0" name="PROPERTY_CARDINALITY" sql:field="PROPERTY_CARDINALITY" type="string"/>
            <xsd:element minOccurs="0" name="MIME_TYPE" sql:field="MIME_TYPE" type="string"/>
            <xsd:element minOccurs="0" name="PROPERTY_IS_VISIBLE" sql:field="PROPERTY_IS_VISIBLE" type="boolean"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""

mdschema_members_xsd = """
<xsd:schema elementFormDefault="qualified"
targetNamespace="urn:schemas-microsoft-com:xml-analysis:rowset"
xmlns:sql="urn:schemas-microsoft-com:xml-sql">
    <xsd:element name="root">
        <xsd:complexType>
            <xsd:sequence maxOccurs="unbounded" minOccurs="0">
                <xsd:element name="row" type="row"/>
            </xsd:sequence>
        </xsd:complexType>
    </xsd:element>
    <xsd:simpleType name="uuid">
        <xsd:restriction base="xsd:string">
            <xsd:pattern value="[0-9a-zA-Z]{8}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{4}-[0-9a-zA-Z]{12}"/>
        </xsd:restriction>
    </xsd:simpleType>
    <xsd:complexType name="xmlDocument">
        <xsd:sequence>
            <xsd:any/>
        </xsd:sequence>
    </xsd:complexType>
    <xsd:complexType name="row">
        <xsd:sequence>
            <xsd:element minOccurs="0" name="CATALOG_NAME" sql:field="CATALOG_NAME" type="string"/>
            <xsd:element minOccurs="0" name="SCHEMA_NAME" sql:field="SCHEMA_NAME" type="string"/>
            <xsd:element minOccurs="0" name="CUBE_NAME" sql:field="CUBE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="DIMENSION_UNIQUE_NAME" sql:field="DIMENSION_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="HIERARCHY_UNIQUE_NAME" sql:field="HIERARCHY_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_UNIQUE_NAME" sql:field="LEVEL_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="LEVEL_NUMBER" sql:field="LEVEL_NUMBER" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="MEMBER_ORDINAL" sql:field="MEMBER_ORDINAL" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="MEMBER_NAME" sql:field="MEMBER_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEMBER_UNIQUE_NAME" sql:field="MEMBER_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="MEMBER_TYPE" sql:field="MEMBER_TYPE" type="int"/>
            <xsd:element minOccurs="0" name="MEMBER_GUID" sql:field="MEMBER_GUID" type="uuid"/>
            <xsd:element minOccurs="0" name="MEMBER_CAPTION" sql:field="MEMBER_CAPTION" type="string"/>
            <xsd:element minOccurs="0" name="CHILDREN_CARDINALITY" sql:field="CHILDREN_CARDINALITY" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="PARENT_LEVEL" sql:field="PARENT_LEVEL" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="PARENT_UNIQUE_NAME" sql:field="PARENT_UNIQUE_NAME" type="string"/>
            <xsd:element minOccurs="0" name="PARENT_COUNT" sql:field="PARENT_COUNT" type="unsignedInt"/>
            <xsd:element minOccurs="0" name="DESCRIPTION" sql:field="DESCRIPTION" type="string"/>
            <xsd:element minOccurs="0" name="EXPRESSION" sql:field="EXPRESSION" type="string"/>
            <xsd:element minOccurs="0" name="MEMBER_KEY" sql:field="MEMBER_KEY" type="string"/>
            <xsd:element minOccurs="0" name="IS_PLACEHOLDERMEMBER" sql:field="IS_PLACEHOLDERMEMBER" type="boolean"/>
            <xsd:element minOccurs="0" name="IS_DATAMEMBER" sql:field="IS_DATAMEMBER" type="boolean"/>
            <xsd:element minOccurs="0" name="SCOPE" sql:field="SCOPE" type="int"/>
        </xsd:sequence>
    </xsd:complexType>
</xsd:schema>
"""
