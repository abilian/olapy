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


cdef cypclass SchemaResponse:
    cyplist[STuple] schema
    RowTuples restrictions

    __init__(self):
        self.schema = cyplist[STuple]()
        self.restrictions = RowTuples()

    void set_name(self, const char* value):
        st = STuple("SchemaName", value)
        self.schema.append(st)

    void set_guid(self, const char* value):
        st = STuple("SchemaGuid", value)
        self.schema.append(st)

    void add_restriction(self, const char* name, const char* tpe):
        """ change structure
        "restrictions": {
            "restriction_names": [
                "MODEL_CATALOG",
                "MODEL_SCHEMA",
                "MODEL_NAME",
                "MODEL_TYPE",
                "SERVICE_NAME",
                "SERVICE_TYPE_ID",
                "MINING_STRUCTURE",
            ],
            "restriction_types": [
                "string",
                "string",
                "string",
                "string",
                "string",
                "unsignedInt",
                "string",
            ],

        =>
            ("MODEL_CATALOG", "string"),
            ("MODEL_SCHEMA", "string"),
            ("MODEL_NAME", "string"),
            ("MODEL_TYPE", "string"),
            ("SERVICE_NAME", "string"),
            ...
        """
        self.restrictions.append(name, tpe)

    void set_restriction_mask(self, const char* value):
        st = STuple("RestrictionsMask", value)
        self.schema.append(st)
