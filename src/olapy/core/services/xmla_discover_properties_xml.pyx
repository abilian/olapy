from olapy.stdlib.string cimport Str
from olapy.stdlib.format cimport format
from olapy.cypxml cimport cypXML
from olapy.cypxml.cypxml cimport Elem
from libc.stdlib  cimport getenv


cdef Str username() nogil:
    cdef char * u
    cdef Str result

    u = getenv("USERNAME")
    if u is not NULL:
        result = Str(u)
        if result.__len__() > 0:
            return result
    return Str("default")


cdef Elem _fill_properties(
    Elem elem,
    Str name,
    Str description,
    Str tpe,
    Str access,
    Str is_required,
    Str value) nogil:

    elem.stag("PropertyName").text(name)
    elem.stag("PropertyDescription").text(description)
    elem.stag("PropertyType").text(tpe)
    elem.stag("PropertyAccessType").text(access)
    elem.stag("IsRequired").text(is_required)
    elem.stag("Value").text(value)


cdef Str properties_xml(
    Str xsd,
    Str name,
    Str description,
    Str tpe,
    Str access,
    Str is_required,
    Str value,
) nogil:
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(0)

    ret = xml.stag("return")
    root = ret.stag("root")
    root.sattr("xmlns", "urn:schemas-microsoft-com:xml-analysis:rowset")
    root.sattr("xmlns:xsd", "http://www.w3.org/2001/XMLSchema")
    root.sattr("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.append(xsd)
    row = root.stag("row")
    if name.__len__() > 0:
        _fill_properties(row, name, description, tpe, access, is_required, value)
    else:
        _fill_properties(
                row, Str("ServerName"), Str("ServerName"),
                Str("string"),
                Str("Read"), Str("false"),
                username()
            )
        _fill_properties(
                row, Str("ProviderVersion"), Str("ProviderVersion"),
                Str("string"),
                Str("Read"), Str("false"),
                Str("0.0.3  25-Nov-2016 07:20:28 GMT")
            )
        _fill_properties(
                row, Str("MdpropMdxSubqueries"), Str("MdpropMdxSubqueries"),
                Str("int"),
                Str("Read"), Str("false"),
                Str("15")
            )
        _fill_properties(
                row, Str("MdpropMdxDrillFunctions"), Str("MdpropMdxDrillFunctions"),
                Str("int"),
                Str("Read"), Str("false"),
                Str("3")
            )
        _fill_properties(
                row, Str("MdpropMdxNamedSets"), Str("MdpropMdxNamedSets"),
                Str("int"),
                Str("Read"), Str("false"),
                Str("15")
            )

    result = xml.dump()
    return result
