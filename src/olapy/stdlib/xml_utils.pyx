from stdlib.string cimport Str
from stdlib._string cimport npos
from libcythonplus.dict cimport cypdict
from libcythonplus.list cimport cyplist
from stdlib.format cimport format


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

cdef void __dict_replace(Str s, cypdict[Str, Str] d) nogil:
    """Replace substrings of a string using a dictionary.

    Return NULL, Change in place.
    """
    for item in d.items():
        replace_all(s, item.first, item.second)

cdef void escape(Str data, cypdict[Str, Str] entities) nogil:
    """Escape '&', '<', and '>' in a string of data.

    You can escape other strings of data by passing a dictionary as
    the optional entities parameter.  The keys and values must all be
    strings; each key will be replaced with its corresponding value.

    Return NULL, change in place.
    """
    # must do ampersand first
    replace_all(data, Str("&"), Str("&amp;"))
    replace_all(data, Str(">"), Str("&gt;"))
    replace_all(data, Str("<"), Str("&lt;"))
    if entities is not NULL:
        __dict_replace(data, entities)

cdef Str escaped(Str data, cypdict[Str, Str] entities) nogil:
    """Escape '&', '<', and '>' in a string of data.

    You can escape other strings of data by passing a dictionary as
    the optional entities parameter.  The keys and values must all be
    strings; each key will be replaced with its corresponding value.

    Return Str, the escaped string.
    """
    cdef Str s

    s = data.copy()
    escape(s, entities)
    return s

cdef void unescape(Str data, cypdict[Str, Str] entities) nogil:
    """Unescape &amp;, &lt;, and &gt; in a string of data.

    You can unescape other strings of data by passing a dictionary as
    the optional entities parameter.  The keys and values must all be
    strings; each key will be replaced with its corresponding value.

    Return NULL, change in place.
    """
    replace_all(data, Str("&lt;"), Str("<"))
    replace_all(data, Str("&gt;"), Str(">"))
    if entities is not NULL:
        __dict_replace(data, entities)
    replace_all(data, Str("&amp;"), Str("&"))

cdef Str unescaped(Str data, cypdict[Str, Str] entities) nogil:
    """Unescape &amp;, &lt;, and &gt; in a string of data.

    You can unescape other strings of data by passing a dictionary as
    the optional entities parameter.  The keys and values must all be
    strings; each key will be replaced with its corresponding value.

    Return Str, the unescaped string.
    """
    cdef Str s

    s = data.copy()
    unescape(s, entities)
    return s

cdef void quoteattr(Str data, cypdict[Str, Str] entities) nogil:
    """Escape and quote an attribute value.

    Escape &, <, and > in a string of data, then quote it for use as
    an attribute value.  The \" character will be escaped as well, if
    necessary.
    You can escape other strings of data by passing a dictionary as
    the optional entities parameter.  The keys and values must all be
    strings; each key will be replaced with its corresponding value.

    Return NULL, change in place.
    """
    cdef cypdict[Str, Str] ent2
    cdef Str q1
    cdef Str q2
    cdef size_t pos
    cdef Str tmp

    q1 = Str("'")
    q2 = Str('"')
    ent2 = cypdict[Str, Str]()
    if entities is not NULL:
        for item in entities.items():
            ent2[item.first] = item.second
    ent2[Str("\n")] = Str("&#10;")
    ent2[Str("\r")] = Str("&#13;")
    ent2[Str("\t")] = Str("&#9;")
    escape(data, ent2)

    pos = data.find(q2)
    if pos == npos:  # not in string
        tmp = format("\"{}\"", data)
        data._str = tmp._str
    else:
        pos = data.find(q1)
        if pos == npos:  # not in string
            tmp = format("'{}'", data)
            data._str = tmp._str
        else:
            replace_all(data, q2, Str("&quot;"))
            tmp = format("\"{}\"", data)
            data._str = tmp._str

cdef Str quotedattr(Str data, cypdict[Str, Str] entities) nogil:
    """Escape and quote an attribute value.

    Escape &, <, and > in a string of data, then quote it for use as
    an attribute value.  The \" character will be escaped as well, if
    necessary.
    You can escape other strings of data by passing a dictionary as
    the optional entities parameter.  The keys and values must all be
    strings; each key will be replaced with its corresponding value.

    Return Str, a quoted string.
    """
    cdef Str s

    s = data.copy()
    quoteattr(s, entities)
    return s

cdef Str nameprep(Str name) nogil:
    """Undo colon mangling
    """
    cdef Str result

    result = name.copy()
    replace_all(result, Str("__"), Str(":"))
    return result

cdef Str concate(cyplist[Str] strings) nogil:
    cdef Str joined
    cdef int total

    joined = Str()
    if strings.__len__() == 0:
        return joined
    total = 0
    for s in strings:
        total += s._str.size()
    joined._str.reserve(total)
    for s in strings:
        joined._str.append(s._str)
    return joined

cdef Str indented(Str content, Str space) nogil:
    cdef cyplist[Str] splitted
    cdef Str internal_indent, joined, result

    splitted = content.split(Str("\n"))
    internal_indent = format("\n{}", space)
    joined = internal_indent.join(splitted)
    result = format("{}{}\n",space, joined)
    return result
