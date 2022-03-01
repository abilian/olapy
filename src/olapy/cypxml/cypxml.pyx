from olapy.stdlib.string cimport Str
from olapy.stdlib.format cimport format
from libcythonplus.dict cimport cypdict
from libcythonplus.list cimport cyplist

from olapy.stdlib.xml_utils cimport escaped, quotedattr, nameprep, concate

from olapy.scheduler.scheduler cimport SequentialMailBox, NullResult, Scheduler


cdef Str to_str(byte_or_string):
    if isinstance(byte_or_string, str):
        return Str(byte_or_string.encode("utf8", "replace"))
    else:
        return Str(bytes(byte_or_string))


def cypxml_to_py_str(xml):
    return xml.dump().decode("utf-8")
