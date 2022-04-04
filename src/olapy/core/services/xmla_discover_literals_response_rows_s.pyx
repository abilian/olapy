from olapy.core.services.structures cimport RowTuples
from libcythonplus.list cimport cyplist


# cdef cyplist[RowTuples] discover_literals_response_rows_l


cdef cyplist[RowTuples] _make_literals_response_rows():
    cdef cyplist[RowTuples] rows
    cdef RowTuples row

    rows = cyplist[RowTuples]()

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_CATALOG_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "24")
    row.append("LiteralNameEnumValue", "2")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_CATALOG_SEPARATOR")
    row.append("LiteralValue", ".")
    row.append("LiteralMaxLength", "0")
    row.append("LiteralNameEnumValue", "3")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_COLUMN_ALIAS")
    row.append("LiteralInvalidChars", "'&quot;[]")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "5")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_COLUMN_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "6")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_CORRELATION_NAME")
    row.append("LiteralInvalidChars", "'&quot;[]")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "7")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_CUBE_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "21")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_DIMENSION_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "22")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_LEVEL_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "24")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_MEMBER_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "25")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_PROCEDURE_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "14")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_PROPERTY_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "26")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_QUOTE_PREFIX")
    row.append("LiteralValue", "[")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "15")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_QUOTE_SUFFIX")
    row.append("LiteralValue", "]")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "28")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_TABLE_NAME")
    row.append("LiteralInvalidChars", ".")
    row.append("LiteralInvalidStartingChars", "0123456789")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "17")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_TEXT_COMMAND")
    row.append("LiteralMaxLength", "-1")
    row.append("LiteralNameEnumValue", "18")
    rows.append(row)

    row = RowTuples()
    row.append("LiteralName", "DBLITERAL_USER_NAME")
    row.append("LiteralMaxLength", "0")
    row.append("LiteralNameEnumValue", "19")
    rows.append(row)

    return rows


discover_literals_response_rows_l = _make_literals_response_rows()
