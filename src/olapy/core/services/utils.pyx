from olapy.stdlib.string cimport Str
from libcythonplus.list cimport cyplist
from olapy.stdlib.format cimport format


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


cdef cyplist[Str] cypstr_copy_slice(cyplist[Str] lst, size_t start, size_t end) nogil:
    cdef cyplist[Str] result

    result = cyplist[Str]()
    if lst._active_iterators == 0:
        for i in range(start, end):
            result._elements.push_back((<Str>lst[i]).copy())
        return result
    else:
        with gil:
            raise RuntimeError("Modifying a list with active iterators")


cdef cyplist[Str] cypstr_copy_slice_from(cyplist[Str] lst, size_t start) nogil:
    return cypstr_copy_slice(lst, start, lst._elements.size())


cdef cyplist[Str] cypstr_copy_slice_to(cyplist[Str] lst, size_t end) nogil:  # size_type
    return cypstr_copy_slice(lst, 0, end)
