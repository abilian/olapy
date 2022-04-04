from olapy.stdlib.string cimport Str
from libcythonplus.list cimport cyplist


cdef cypclass STuple:
    Str key
    Str value

    __init__(self, const char* key, const char* value):
        self.key = Str(key)
        self.value = Str(value)


cdef cypclass RowTuples:
    cyplist[STuple] row

    __init__(self):
        self.row = cyplist[STuple]()

    void append(self, const char* key, const char* value):
        cdef STuple st

        st = STuple(key, value)
        self.row.append(st)
