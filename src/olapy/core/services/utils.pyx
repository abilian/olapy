from libc.stdlib  cimport getenv
from libcythonplus.list cimport cyplist
from olapy.stdlib.string cimport Str, isspace
from olapy.stdlib._string cimport npos
from olapy.stdlib.format cimport format


cdef int replace_all(Str src, Str pattern, Str content) nogil:
    """Replace all occurences of 'pattern' in 'src' by 'content'.

    Return number of changes, Change in place.
    """
    cdef size_t start
    cdef int count

    count = 0
    if pattern.__len__() == 0:
        return count
    if src.__len__() == 0:
        return count
    start = 0
    while 1:
        start = src.find(pattern, start)
        if start == npos:
            return count
        count += 1
        src._str.replace(start, pattern.__len__(), content._str)
        start += content.__len__()


cdef int replace_one(Str src, Str pattern, Str content) nogil:
    """Replace first occurence of 'pattern' in 'src' by 'content'.

    Return number of changes, Change in place.
    """
    cdef size_t start

    start = src.find(pattern)
    if start == npos:
        return 0
    src._str.replace(start, pattern.__len__(), content._str)
    return 1


cdef Str stripped(Str s) nogil:
    """return stripped string
    """
    cdef int start, end

    if s is NULL:
        return NULL
    if s._str.size() == 0:
        return Str("")
    start = 0
    end = s._str.size()
    while start < end and isspace(s[start]):
        start += 1
    while end > start and isspace(s[end - 1]):
        end -= 1
    if end <= start:
        return Str("")
    return s.substr(start, end)


cdef cyplist[Str] cypstr_split_tuple(Str tupl) nogil:
    """Split Tuple (Cython+ Str) into items.

    example::

         input : '[Geography].[Geography].[Continent].[Europe]'

         output : ['Geography', 'Geography', 'Continent', 'Europe']

    :param tupl: MDX Tuple as Cython+ Str
    :return: cyplist[Str] list of items
    """
    cdef cyplist[Str] lst

    lst = stripped(tupl).split(Str("].["))
    if lst.__len__() == 0:
        return lst
    replace_one(lst[0], Str("["), Str(""))
    replace_one(lst[lst.__len__() - 1], Str("]"), Str(""))
    return lst


cdef Str username() nogil:
    cdef char * u
    cdef Str result

    u = getenv("USERNAME")
    if u is not NULL:
        result = Str(u)
        if result.__len__() > 0:
            return result
    return Str("default")


cdef Str bracket(Str s) nogil:
    return format("[{}]", s)


cdef Str bracket2(Str s) nogil:
    return format("[{}].[{}]", s)


cdef Str dot_bracket(cyplist[Str] lst) nogil:
    cdef cyplist[Str] tmp
    cdef Str dot, result

    dot = Str(".")
    tmp = cyplist[Str]()
    for s in lst:
        tmp.append(format("[{}]", s))
    result = dot.join(tmp)
    return result


cdef cyplist[Str] pylist_to_cyplist(list py_str_lst):
    cdef cyplist[Str] result

    result = cyplist[Str]()
    for s in py_str_lst:
        result.append(Str(s.encode("utf8", "replace")))
    return result


cdef cyplist[Str] cypstr_copy_slice(cyplist[Str] lst, size_t start, int end) nogil:
    cdef cyplist[Str] result
    cdef size_t end2
    cdef int tmp

    if not (lst._active_iterators == 0):
        with gil:
            raise RuntimeError("Modifying a list with active iterators")

    result = cyplist[Str]()
    if end < 0:
        tmp = <int> lst.__len__() + end
        if tmp < 0:
            return result
    end2 = <size_t> end
    for i in range(start, end):
        result._elements.push_back((<Str>lst[i]).copy())
    return result


cdef cyplist[Str] cypstr_copy_slice_from(cyplist[Str] lst, size_t start) nogil:
    return cypstr_copy_slice(lst, start, <int> lst._elements.size())


cdef cyplist[Str] cypstr_copy_slice_to(cyplist[Str] lst, int end) nogil:  # size_type
    return cypstr_copy_slice(lst, 0, end)
