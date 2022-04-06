from olapy.stdlib.string cimport Str
from libcythonplus.list cimport cyplist


cdef cyplist[Str] _make_properties_names() nogil:
    cdef cyplist[Str] lst

    lst = cyplist[Str]()
    lst.append(Str("FONT_FLAGS"))
    lst.append(Str("LANGUAGE"))
    lst.append(Str("style"))
    lst.append(Str("ACTION_TYPE"))
    lst.append(Str("FONT_SIZE"))
    lst.append(Str("FORMAT_STRING"))
    lst.append(Str("className"))
    lst.append(Str("UPDATEABLE"))
    lst.append(Str("BACK_COLOR"))
    lst.append(Str("CELL_ORDINAL"))
    lst.append(Str("FONT_NAME"))
    lst.append(Str("VALUE"))
    lst.append(Str("FORMATTED_VALUE"))
    lst.append(Str("FORE_COLOR"))
    return lst


cdef cyplist[Str] _make_properties_captions() nogil:
    cdef cyplist[Str] lst

    lst = cyplist[Str]()
    lst.append(Str("FONT_FLAGS"))
    lst.append(Str("LANGUAGE"))
    lst.append(Str("style"))
    lst.append(Str("ACTION_TYPE"))
    lst.append(Str("FONT_SIZE"))
    lst.append(Str("FORMAT_STRING"))
    lst.append(Str("className"))
    lst.append(Str("UPDATEABLE"))
    lst.append(Str("BACK_COLOR"))
    lst.append(Str("CELL_ORDINAL"))
    lst.append(Str("FONT_NAME"))
    lst.append(Str("VALUE"))
    lst.append(Str("FORMATTED_VALUE"))
    lst.append(Str("FORE_COLOR"))
    return lst


cdef cyplist[Str] _make_properties_datas() nogil:
    cdef cyplist[Str] lst

    lst = cyplist[Str]()
    lst.append(Str("3"))
    lst.append(Str("19"))
    lst.append(Str("130"))
    lst.append(Str("19"))
    lst.append(Str("18"))
    lst.append(Str("130"))
    lst.append(Str("130"))
    lst.append(Str("19"))
    lst.append(Str("19"))
    lst.append(Str("19"))
    lst.append(Str("130"))
    lst.append(Str("12"))
    lst.append(Str("130"))
    lst.append(Str("19"))
    return lst


properties_names = _make_properties_names()
properties_captions = _make_properties_captions()
properties_datas = _make_properties_datas()
