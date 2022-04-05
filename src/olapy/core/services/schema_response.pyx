from olapy.stdlib.string cimport Str
from libcythonplus.list cimport cyplist
from olapy.core.services.structures cimport SchemaResponse
from olapy.cypxml cimport cypXML

discover_schema_rowsets_xsd_s = Str("""
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
""")

cdef Str discover_schema_rowsets_response_str(cyplist[SchemaResponse] rows):
    """Generate the names, restrictions, description, and other information
    for all enumeration values and any additional provider-specific
    enumeration values supported by OlaPy.

    :param rows:
    :return: xmla response as string
    """
    cdef cypXML xml
    cdef Str result

    # xml = xmlwitch.Builder()
    xml = cypXML()
    xml.set_max_depth(2)
    # with xml["return"]:
    #     with xml.root(
    #         xmlns="urn:schemas-microsoft-com:xml-analysis:rowset",
    #         **{
    #             "xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
    #             "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    #         },
    #     ):
    #         xml.write(discover_schema_rowsets_xsd)
    #         for resp_row in rows:
    #             with xml.row:
    #                 xml.SchemaName(resp_row["SchemaName"])
    #                 xml.SchemaGuid(resp_row["SchemaGuid"])
    #                 for idx, restriction in enumerate(
    #                     resp_row["restrictions"]["restriction_names"]
    #                 ):
    #                     with xml.Restrictions:
    #                         xml.Name(restriction)
    #                         xml.Type(
    #                             resp_row["restrictions"]["restriction_types"][
    #                                 idx
    #                             ]
    #                         )
    #
    #                 xml.RestrictionsMask(resp_row["RestrictionsMask"])
    ret = xml.stag("return")
    root = ret.stag("root")
    root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
    root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
    root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.append(discover_schema_rowsets_xsd_s)
    # for resp_row in rows:
    #     row = root.stag("row")
    #     row.stag("SchemaName").text(to_str(resp_row["SchemaName"]))
    #     row.stag("SchemaGuid").text(to_str(resp_row["SchemaGuid"]))
    #     for idx, restriction in enumerate(
    #         resp_row["restrictions"]["restriction_names"]
    #     ):
    #         r = row.stag("Restrictions")
    #         r.stag("NAME").text(to_str(restriction))
    #         r.stag("Type").text(to_str(
    #                     resp_row["restrictions"]["restriction_types"][idx]
    #                     ))
    #     row.stag("RestrictionsMask").text(to_str(resp_row["RestrictionsMask"]))
    for schema_row in rows:
        row = root.stag("row")
        row.stag("SchemaName").text(schema_row.name)
        row.stag("SchemaGuid").text(schema_row.guid)
        for restriction in schema_row.restrictions.row:
            r = row.stag("Restrictions")
            r.stag("NAME").text(restriction.key)
            r.stag("Type").text(restriction.value)
        row.stag("RestrictionsMask").text(schema_row.mask)

    result = xml.dump()
    return result
