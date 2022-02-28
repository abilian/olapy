from libcythonplus.list cimport cyplist
from stdlib._string cimport string, string_view, hash_string, stoi, transform


cdef extern from "<cctype>" namespace "std" nogil:

    int isalnum(int c)
    int isalpha(int c)
    int isblank(int c)
    int iscntrl(int c)
    int isdigit(int c)
    int isgraph(int c)
    int islower(int c)
    int isprint(int c)
    int ispunct(int c)
    int isspace(int c)
    int isupper(int c)
    int isxdigit(int c)
    int tolower(int c)
    int toupper(int c)


cdef cypclass Str "Cy_Str":
    string _str

    __init__(self, const char *s):
        self._str = string(s)

    size_t __len__(self):
        return self._str.size()

    bint __eq__(self, Str other):
        return self._str == other._str

    bint __ne__(self, Str other):
        return self._str != other._str

    size_t __hash__(self):
        return hash_string()(self._str)

    char __getitem__(self, int index) except 0:
        cdef int end = self._str.size()
        cdef int idx = index
        if index < 0:
            index = -index
            idx = end - index
        if index >= end:
            with gil:
                raise ValueError('index out of range')
        return self._str[idx]

    int find(self, Str s, size_t start=0, size_t stop=0):
        if start < stop and stop <= self._str.size():
            sw = string_view(self._str.data(), stop)
            return sw.find(s._str.data(), start)
        return self._str.find(s._str, start)

    Str substr(self, int start=0, int stop=0) except NULL:
        cdef int end = self._str.size()
        cdef int tmp = stop
        if stop <= 0:
            stop = -stop
            tmp = end - stop
        if stop > end:
            with gil:
                raise ValueError('substr bounds out of range')
        stop = tmp
        if start < 0:
            start = -start
            if start > end:
                with gil:
                    raise ValueError('substr bounds out of range')
            start = end - start
        if start > stop:
            with gil:
                raise ValueError('substr bounds out of order')
        cdef size_t size = stop - start
        result = Str()
        result._str = self._str.substr(start, size)
        return result

    cyplist[Str] split(self, Str delimiter=NULL) except NULL:
        cdef cyplist[Str] result = cyplist[Str]()
        cdef int start = 0
        cdef int stop = 0
        cdef int end = self._str.size()
        if delimiter is NULL:
            while True:
                while stop < end and isspace(self._str[stop]):
                    stop += 1
                start = stop
                if start >= end:
                    return result
                stop = start + 1
                while stop < end and not isspace(self._str[stop]):
                    stop += 1
                result.append(self.substr(start, stop))
        else:
            while start < end:
                stop = self.find(delimiter, start)
                if stop == -1:
                    stop = end
                result.append(self.substr(start, stop))
                start = stop + delimiter._str.size()
            return result

    Str join(self, cyplist[Str] strings) except NULL:
        cdef Str joined = Str()
        if strings is NULL or strings.__len__() == 0:
            return joined
        last = strings[strings.__len__() -1]
        del strings[strings.__len__() - 1]
        cdef int total = last._str.size()
        for s in strings:
            total += s._str.size()
            total += self._str.size()
        joined._str.reserve(total)
        for s in strings:
            joined._str.append(s._str)
            joined._str.append(self._str)
        joined._str.append(last._str)
        return joined

    Str lower(self):
        cdef Str result = new Str()
        result._str = self._str
        transform(result._str.begin(), result._str.end(), result._str.begin(), tolower)
        return result

    Str __add__(self, Str other):
        cdef Str result = new Str()
        result._str = self._str + other._str
        return result

    int __int__(self) except +:
        return stoi(self._str)

    string_view __string_view__(self):
        return string_view(self._str.data(), self._str.size())

    const char * bytes(self):
        return self._str.c_str()

    Str copy(self):
        cdef Str result = new Str()
        result._str = self._str
        return result

    @staticmethod
    const char * to_c_str(Str s):
        if s is NULL:
            return NULL
        return s._str.data()

    @staticmethod
    Str copy_iso(iso Str s):
        cdef Str result = new Str()
        result._str = s._str
        return result
