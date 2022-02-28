from stdlib.string cimport Str
from libcythonplus.dict cimport cypdict
from libcythonplus.list cimport cyplist
from stdlib.format cimport format

from .stdlib.xml_utils cimport replace_one, replace_all
from .stdlib.xml_utils cimport escape, escaped, unescape, unescaped
from .stdlib.xml_utils cimport quoteattr, quotedattr, nameprep
from .stdlib.xml_utils cimport concate, indented

cdef bint test_replace_one_1():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("abc")
    content = Str("ABC")
    expected = Str("some str ABC abc")
    expected_res = 1
    result = replace_one(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_one_2():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("")
    content = Str("ABC")
    expected = Str("ABCsome str abc abc")
    expected_res = 1
    result = replace_one(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_one_3():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("not found")
    content = Str("ABC")
    expected = Str("some str abc abc")
    expected_res = 0
    result = replace_one(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_one_4():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("abc")
    content = Str("x")
    expected = Str("some str x abc")
    expected_res = 1
    result = replace_one(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_one_5():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("abc")
    content = Str("")
    expected = Str("some str  abc")
    expected_res = 1
    result = replace_one(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_one_6():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("abc")
    content = Str("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    expected = Str("some str xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx abc")
    expected_res = 1
    result = replace_one(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_one_7():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("")
    pattern = Str("abc")
    content = Str("ABC")
    expected = Str("")
    expected_res = 0
    result = replace_one(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_one_8():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("")
    pattern = Str("")
    content = Str("ABC")
    expected = Str("ABC")
    expected_res = 1
    result = replace_one(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_replace_all_1():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("abc")
    content = Str("ABC")
    expected = Str("some str ABC ABC")
    expected_res = 2
    result = replace_all(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_all_2():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("")
    content = Str("ABC")
    expected = Str("some str abc abc")
    expected_res = 0
    result = replace_all(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_all_3():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("not found")
    content = Str("ABC")
    expected = Str("some str abc abc")
    expected_res = 0
    result = replace_all(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_all_4():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("abc")
    content = Str("x")
    expected = Str("some str x x")
    expected_res = 2
    result = replace_all(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_all_5():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("abc")
    content = Str("")
    expected = Str("some str  ")
    expected_res = 2
    result = replace_all(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_all_6():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("some str abc abc")
    pattern = Str("abc")
    content = Str("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    expected = Str("some str xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    expected_res = 2
    result = replace_all(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_all_7():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("")
    pattern = Str("abc")
    content = Str("ABC")
    expected = Str("")
    expected_res = 0
    result = replace_all(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

cdef bint test_replace_all_8():
    cdef Str src
    cdef Str pattern
    cdef Str content
    cdef Str expected
    cdef int result
    cdef int expected_res

    src = Str("")
    pattern = Str("")
    content = Str("ABC")
    expected = Str("")
    expected_res = 0
    result = replace_all(src, pattern, content)
    if result == expected_res and src == expected:
        return 1
    print("-------------------------------------")
    print(result)
    print(src.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_escape_1():
    cdef Str src
    cdef Str expected

    src = Str("some ééé abc abc")
    expected = Str("some ééé abc abc")
    escape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_2():
    cdef Str src
    cdef Str expected

    src = Str("some ééé & && &a &b")
    expected = Str("some ééé &amp; &amp;&amp; &amp;a &amp;b")
    escape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_3():
    cdef Str src
    cdef Str expected

    src = Str("some ééé >")
    expected = Str("some ééé &gt;")
    escape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_4():
    cdef Str src
    cdef Str expected

    src = Str("some ééé <")
    expected = Str("some ééé &lt;")
    escape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_5():
    cdef Str src
    cdef Str expected

    src = Str("some ééé < & &< > >> & >")
    expected = Str("some ééé &lt; &amp; &amp;&lt; &gt; &gt;&gt; &amp; &gt;")
    escape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_6():
    cdef Str src
    cdef Str expected

    src = Str("")
    expected = Str("")
    escape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_7():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] ent

    ent = cypdict[Str, Str]()
    ent[Str("é")] = Str("&eacute;")
    ent[Str("à")] = Str("&agrave;")

    src = Str("some ééé abc abc")
    expected = Str("some &eacute;&eacute;&eacute; abc abc")
    escape(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_8():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] ent

    ent = cypdict[Str, Str]()

    src = Str("some ééé abc abc")
    expected = Str("some ééé abc abc")
    escape(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_9():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] ent

    ent = cypdict[Str, Str]()
    ent[Str("é")] = Str("&eacute;")
    ent[Str("à")] = Str("&agrave;")

    src = Str("some éàé abc abc")
    expected = Str("some &eacute;&agrave;&eacute; abc abc")
    escape(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_escape_10():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] ent

    ent = cypdict[Str, Str]()
    ent[Str("é")] = Str("&eacute;")
    ent[Str("à")] = Str("&agrave;")

    src = Str("some & éàé > abc abc")
    expected = Str("some &amp; &eacute;&agrave;&eacute; &gt; abc abc")
    escape(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_escaped_1():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result

    src = Str("some ééé abc abc")
    src2 = Str("some ééé abc abc")
    expected = Str("some ééé abc abc")
    result = escaped(src, NULL)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_escaped_2():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result

    src = Str("some ééé & && &a &b")
    src2 = Str("some ééé & && &a &b")
    expected = Str("some ééé &amp; &amp;&amp; &amp;a &amp;b")
    result = escaped(src, NULL)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_escaped_3():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result
    cdef cypdict[Str, Str] ent

    ent = cypdict[Str, Str]()
    ent[Str("é")] = Str("&eacute;")
    ent[Str("à")] = Str("&agrave;")

    src = Str("some & éàé > abc abc")
    src2 = Str("some & éàé > abc abc")
    expected = Str("some &amp; &eacute;&agrave;&eacute; &gt; abc abc")
    result = escaped(src, ent)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_unescape_1():
    cdef Str src
    cdef Str expected

    src = Str("some ééé abc abc")
    expected = Str("some ééé abc abc")
    unescape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_2():
    cdef Str src
    cdef Str expected

    src = Str("some ééé &amp; &amp;&amp; &amp;a &amp;b")
    expected = Str("some ééé & && &a &b")
    unescape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_3():
    cdef Str src
    cdef Str expected

    src = Str("some ééé &gt;")
    expected = Str("some ééé >")
    unescape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_4():
    cdef Str src
    cdef Str expected

    src = Str("some ééé &lt;")
    expected = Str("some ééé <")
    unescape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_5():
    cdef Str src
    cdef Str expected

    src = Str("some ééé &lt; &amp; &amp;&lt; &gt; &gt;&gt; &amp; &gt;")
    expected = Str("some ééé < & &< > >> & >")
    unescape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_6():
    cdef Str src
    cdef Str expected

    src = Str("some ééé & <> abc abc")
    expected = Str("some ééé & <> abc abc")
    unescape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_7():
    cdef Str src
    cdef Str expected

    src = Str("")
    expected = Str("")
    unescape(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_8():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] end

    ent = cypdict[Str, Str]()
    ent[Str("&agrave;")] = Str("à")
    ent[Str("&eacute;")] = Str("é")

    src = Str("some ééé abc abc")
    expected = Str("some ééé abc abc")
    unescape(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_9():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] end

    ent = cypdict[Str, Str]()

    src = Str("some ééé abc abc")
    expected = Str("some ééé abc abc")
    unescape(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_unescape_10():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] end

    ent = cypdict[Str, Str]()
    ent[Str("&agrave;")] = Str("à")
    ent[Str("&eacute;")] = Str("é")

    src = Str("some &amp; &eacute;&agrave;&eacute; &gt; abc abc")
    expected = Str("some & éàé > abc abc")
    unescape(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_unescaped_1():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result

    src = Str("some ééé abc abc")
    src2 = Str("some ééé abc abc")
    expected = Str("some ééé abc abc")
    result = unescaped(src, NULL)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_unescaped_2():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result

    src = Str("some ééé &amp; &amp;&amp; &amp;a &amp;b")
    src2 = Str("some ééé &amp; &amp;&amp; &amp;a &amp;b")
    expected = Str("some ééé & && &a &b")
    result = unescaped(src, NULL)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_unescaped_3():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result

    ent = cypdict[Str, Str]()
    ent[Str("&agrave;")] = Str("à")
    ent[Str("&eacute;")] = Str("é")

    src = Str("some &amp; &eacute;&agrave;&eacute; &gt; abc abc")
    src2 = Str("some &amp; &eacute;&agrave;&eacute; &gt; abc abc")
    expected = Str("some & éàé > abc abc")
    result = unescaped(src, ent)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_quoteattr_1():
    cdef Str src
    cdef Str expected

    src = Str("some ééé abc abc")
    expected = Str('"some ééé abc abc"')
    quoteattr(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_quoteattr_2():
    cdef Str src
    cdef Str expected

    src = Str("some 'ééé' abc abc")
    expected = Str('"some \'ééé\' abc abc"')
    quoteattr(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_quoteattr_3():
    cdef Str src
    cdef Str expected

    src = Str('some "ééé" abc abc')
    expected = Str("'some \"ééé\" abc abc'")
    quoteattr(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_quoteattr_4():
    cdef Str src
    cdef Str expected

    src = Str("")
    expected = Str('""')
    quoteattr(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_quoteattr_5():
    cdef Str src
    cdef Str expected

    src = Str(" \"aaa\" 'bbb' ")
    expected = Str("\" &quot;aaa&quot; 'bbb' \"")
    quoteattr(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_quoteattr_6():
    cdef Str src
    cdef Str expected

    src = Str(" \n\n\t\r \"aaa\" 'bbb' ")
    expected = Str("\" &#10;&#10;&#9;&#13; &quot;aaa&quot; 'bbb' \"")
    quoteattr(src, NULL)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_quoteattr_7():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] ent

    ent = cypdict[Str, Str]()

    src = Str(" \n\n\t\r \"aaa\" 'bbb' ")
    expected = Str("\" &#10;&#10;&#9;&#13; &quot;aaa&quot; 'bbb' \"")
    quoteattr(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

cdef bint test_quoteattr_8():
    cdef Str src
    cdef Str expected
    cdef cypdict[Str, Str] ent

    ent = cypdict[Str, Str]()
    ent[Str("é")] = Str("&eacute;")
    ent[Str("à")] = Str("&agrave;")

    src = Str("some 'ééé' à.\n ")
    expected = Str('"some \'&eacute;&eacute;&eacute;\' &agrave;.&#10; "')
    quoteattr(src, ent)
    if src == expected:
        return 1
    print("-------------------------------------")
    print(src.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_quotedattr_1():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result

    src = Str("some ééé abc abc")
    src2 = Str("some ééé abc abc")
    expected = Str('"some ééé abc abc"')
    result = quotedattr(src, NULL)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_quotedattr_2():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result

    src = Str("some 'ééé' abc abc")
    src2 = Str("some 'ééé' abc abc")
    expected = Str('"some \'ééé\' abc abc"')
    result = quotedattr(src, NULL)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_quotedattr_3():
    cdef Str src
    cdef Str src2
    cdef Str expected
    cdef Str result
    cdef cypdict[Str, Str] ent

    ent = cypdict[Str, Str]()
    ent[Str("é")] = Str("&eacute;")
    ent[Str("à")] = Str("&agrave;")

    src = Str("some 'ééé' à.\n ")
    src2 = Str("some 'ééé' à.\n ")
    expected = Str('"some \'&eacute;&eacute;&eacute;\' &agrave;.&#10; "')
    result = quotedattr(src, ent)
    if result == expected and src == src2:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_nameprep_1():
    cdef Str src
    cdef Str expected
    cdef Str result

    src = Str("name")
    expected = Str("name")
    result = nameprep(src)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_nameprep_2():
    cdef Str src
    cdef Str expected
    cdef Str result

    src = Str("")
    expected = Str("")
    result = nameprep(src)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_nameprep_3():
    cdef Str src
    cdef Str expected
    cdef Str result

    src = Str("space__name")
    expected = Str("space:name")
    result = nameprep(src)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_concate_1():
    cdef cyplist[Str] strings
    cdef Str expected
    cdef Str result

    strings = cyplist[Str]()
    expected = Str("")
    result = concate(strings)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_concate_2():
    cdef cyplist[Str] strings
    cdef Str expected
    cdef Str result

    strings = cyplist[Str]()
    strings.append(Str("aaa"))
    expected = Str("aaa")
    result = concate(strings)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_concate_3():
    cdef cyplist[Str] strings
    cdef Str expected
    cdef Str result

    strings = cyplist[Str]()
    strings.append(Str("aaa"))
    strings.append(Str("bbb"))
    expected = Str("aaabbb")
    result = concate(strings)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

#############################################################################

cdef bint test_indented_1():
    cdef Str content, space
    cdef Str expected
    cdef Str result

    space = Str("  ")
    content = Str("aaa")
    expected = Str("  aaa\n")
    result = indented(content, space)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_indented_2():
    cdef Str content, space
    cdef Str expected
    cdef Str result

    space = Str("  ")
    content = Str("aaa\nbbb\n")
    expected = Str("  aaa\n  bbb\n")
    result = indented(content, space)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

cdef bint test_indented_3():
    cdef Str content, space
    cdef Str expected
    cdef Str result

    space = Str("  ")
    content = Str("aaa\n  bbb\naaa\n")
    expected = Str("  aaa\n    bbb\n  aaa\n")
    result = indented(content, space)
    if result == expected:
        return 1
    print("-------------------------------------")
    print(result.bytes())
    raise RuntimeError()

#############################################################################

def main():
    print("-------------------------------------")
    print("Test xml_utils.")
    test_replace_one_1()
    test_replace_one_2()
    test_replace_one_3()
    test_replace_one_4()
    test_replace_one_5()
    test_replace_one_6()
    test_replace_one_7()
    test_replace_one_8()
    test_replace_all_1()
    test_replace_all_2()
    test_replace_all_3()
    test_replace_all_4()
    test_replace_all_5()
    test_replace_all_6()
    test_replace_all_7()
    test_replace_all_8()
    test_escape_1()
    test_escape_2()
    test_escape_3()
    test_escape_4()
    test_escape_5()
    test_escape_6()
    test_escape_7()
    test_escape_8()
    test_escape_9()
    test_escape_10()
    test_escaped_1()
    test_escaped_2()
    test_escaped_3()
    test_unescape_1()
    test_unescape_2()
    test_unescape_3()
    test_unescape_4()
    test_unescape_5()
    test_unescape_6()
    test_unescape_7()
    test_unescape_8()
    test_unescape_9()
    test_unescape_10()
    test_unescaped_1()
    test_unescaped_2()
    test_unescaped_3()
    test_quoteattr_1()
    test_quoteattr_2()
    test_quoteattr_3()
    test_quoteattr_4()
    test_quoteattr_5()
    test_quoteattr_6()
    test_quoteattr_7()
    test_quoteattr_8()
    test_quotedattr_1()
    test_quotedattr_2()
    test_quotedattr_3()
    test_nameprep_1()
    test_nameprep_2()
    test_nameprep_3()
    test_concate_1()
    test_concate_2()
    test_concate_3()
    test_indented_1()
    test_indented_2()
    test_indented_3()
    print("Done.")
