from stdlib.string cimport Str
from stdlib.format cimport format
from libcythonplus.dict cimport cypdict
from libcythonplus.list cimport cyplist

from .stdlib.xml_utils cimport escaped, quotedattr, nameprep, concate

from libc.stdio cimport *

from .cypxml cimport cypXML

from os.path import abspath, dirname, join


base_path = abspath(dirname(__file__))


cdef Str read_file(Str path) nogil:
    cdef FILE * file
    cdef cyplist[Str] str_buffer
    cdef char buffer[8192]
    cdef bint eof = False
    cdef int size

    str_buffer = cyplist[Str]()
    file = fopen(path._str.c_str(), "rb")
    if file is NULL:
        with gil:
            raise RuntimeError("file not found %s" % path.bytes())
    while not eof:
        size = fread(buffer, 1, 8192, file)
        if size != 8192:
            if ferror(file):
                with gil:
                    raise RuntimeError("file read error %s" % path.bytes())
            eof = True
        if size == 8192:
            str_buffer.append(Str(buffer))
        else:
            str_buffer.append(Str(buffer).substr(0, size))
    fclose(file)
    result = concate(str_buffer)
    return result

cdef Str test_content(name):
    cdef Str path

    path = Str(join(base_path, "expected", name).encode("utf8"))
    return read_file(path)

##############################################################################

cdef void print_err(Str expected, Str result, name="", depth=3):
    print()
    print(f"== Error: {name} max_depth:{depth}")
    print("----- result:")
    print(result.bytes())
    print("----- result:")
    print(result.bytes().decode("utf8"))
    print("----- expected:")
    print(expected.bytes().decode("utf8"))
    print("-----")

##############################################################################

cdef bint test_min(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)

    result = xml.dump()
    expected = Str("")

    if result == expected:
        return 1
    print_err(expected, result, "test_min", xml.max_depth)
    raise RuntimeError()

cdef bint test_version(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.init_version(Str("1.0"))

    result = xml.dump()
    expected = Str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_version", xml.max_depth)
    raise RuntimeError()

cdef bint test_sversion(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.init_sversion("1.0")

    result = xml.dump()
    expected = Str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_sversion", xml.max_depth)
    raise RuntimeError()

cdef bint test_insert_content(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.insert_content(Str("<specific content1>\n"))
    xml.insert_content(Str("<specific content2>\n"))
    xml.init_version(Str("1.0"))

    result = xml.dump()
    expected = Str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<specific content1>\n<specific content2>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_insert_content", xml.max_depth)
    raise RuntimeError()

cdef bint test_insert_scontent(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.insert_scontent("<specific content1>\n")
    xml.insert_scontent("<specific content2>\n")
    xml.init_version(Str("1.0"))

    result = xml.dump()
    expected = Str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<specific content1>\n<specific content2>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_insert_scontent", xml.max_depth)
    raise RuntimeError()

cdef bint test_set_indent_space(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.set_indent_space(Str('---'))
    t = xml.tag(Str("tag"))
    t.tag(Str("elem"))
    e = t.tag(Str("elem"))
    e.tag(Str("sub"))
    e.tag(Str("sub"))

    result = xml.dump()
    expected = Str("<tag>\n---<elem />\n---<elem>\n------<sub />\n------<sub />\n---</elem>\n</tag>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_set_indent_space", xml.max_depth)
    raise RuntimeError()

cdef bint test_set_sindent_space(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.set_sindent_space('---')
    t = xml.tag(Str("tag"))
    t.tag(Str("elem"))
    e = t.tag(Str("elem"))
    e.tag(Str("sub"))
    e.tag(Str("sub"))

    result = xml.dump()
    expected = Str("<tag>\n---<elem />\n---<elem>\n------<sub />\n------<sub />\n---</elem>\n</tag>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_set_sindent_space", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_tag(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.tag(Str("tag"))

    result = xml.dump()
    expected = Str("<tag />\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_tag", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_stag(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.stag("tag")

    result = xml.dump()
    expected = Str("<tag />\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_stag", xml.max_depth)
    raise RuntimeError()

cdef bint test_2_tag(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.tag(Str("tag"))
    xml.tag(Str("tig"))

    result = xml.dump()
    expected = Str("<tag />\n<tig />\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_2_tag", xml.max_depth)
    raise RuntimeError()

cdef bint test_3_tag(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.tag(Str("tag"))
    xml.tag(Str("tig"))
    xml.tag(Str("tag"))

    result = xml.dump()
    expected = Str("<tag />\n<tig />\n<tag />\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_3_tag", xml.max_depth)
    raise RuntimeError()

cdef bint test_3_tag_stag(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.tag(Str("tag"))
    xml.stag("tig")
    xml.stag("tag")

    result = xml.dump()
    expected = Str("<tag />\n<tig />\n<tag />\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_3_tag_stag", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_tag_text(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t = xml.tag(Str("tag"))
    t.text(Str("content."))

    result = xml.dump()
    expected = Str("<tag>content.</tag>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_tag_text", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_tag_stext(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t = xml.tag(Str("tag"))
    t.stext("content.")

    result = xml.dump()
    expected = Str("<tag>content.</tag>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_tag_stext", xml.max_depth)
    raise RuntimeError()

cdef bint test_2_tag_text(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t = xml.tag(Str("tag1"))
    t.text(Str("content1."))
    t = xml.tag(Str("tag2"))
    t.text(Str("content2."))

    result = xml.dump()
    expected = Str("<tag1>content1.</tag1>\n<tag2>content2.</tag2>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_2_tag_text", xml.max_depth)
    raise RuntimeError()

cdef bint test_2_tag_text_mod(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t = xml.tag(Str("tag1"))
    t2 = xml.tag(Str("tag2"))
    t.text(Str("content1."))
    t2.text(Str("content2."))

    result = xml.dump()
    expected = Str("<tag1>content1.</tag1>\n<tag2>content2.</tag2>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_2_tag_text_mod", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_attr(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t = xml.tag(Str("tag"))
    t.attr(Str("key"), Str("value"))

    result = xml.dump()
    expected = Str("<tag key=\"value\" />\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_attr", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_sattr(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t = xml.tag(Str("tag"))
    t.sattr("key", "value")

    result = xml.dump()
    expected = Str("<tag key=\"value\" />\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_sattr", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_attr_text_to_escape(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t = xml.tag(Str("tag"))
    t.attr(Str("key__aa"), Str("value with & > 4"))
    t.text(Str("text with <>"))

    result = xml.dump()
    expected = Str("<tag key:aa=\"value with &amp; &gt; 4\">text with &lt;&gt;</tag>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_attr_text_to_escape", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_embed(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t1 = xml.tag(Str("tag1"))
    t2 = t1.tag(Str("tag2"))
    t3 = t2.tag(Str("tag3"))
    t32 = t2.tag(Str("tag3bis"))

    result = xml.dump()
    expected = Str("<tag1>\n  <tag2>\n    <tag3 />\n    <tag3bis />\n  </tag2>\n</tag1>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_embed", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_embed_attr_text(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t1 = xml.tag(Str("tag1"))
    t2 = t1.tag(Str("tag2"))
    t2.attr(Str("key"), Str("value"))
    t3 = t2.tag(Str("tag3"))
    t3.text(Str("description 3"))
    t32 = t2.tag(Str("tag3bis"))
    t32.text(Str("description & 3bis"))
    t32.attr(Str("x"), Str("y"))

    result = xml.dump()
    expected = Str("<tag1>\n  <tag2 key=\"value\">\n    <tag3>description 3</tag3>\n    <tag3bis x=\"y\">description &amp; 3bis</tag3bis>\n  </tag2>\n</tag1>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_embed_attr_text", xml.max_depth)
    raise RuntimeError()

cdef bint test_2_embed_attr_text(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t1 = xml.tag(Str("tag1"))
    t2 = t1.tag(Str("tag2"))
    t2.attr(Str("key"), Str("value"))
    t3 = t2.tag(Str("tag3"))
    t3.text(Str("description 3"))
    t32 = t2.tag(Str("tag3bis"))
    t32.text(Str("description & 3bis"))
    t32.attr(Str("x"), Str("y"))
    t32.tag(Str("elem"))
    t32.tag(Str("elem"))
    t32.tag(Str("elem"))

    result = xml.dump()
    expected = Str("<tag1>\n  <tag2 key=\"value\">\n    <tag3>description 3</tag3>\n    <tag3bis x=\"y\">description &amp; 3bis\n      <elem />\n      <elem />\n      <elem />\n    </tag3bis>\n  </tag2>\n</tag1>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_2_embed_attr_text", xml.max_depth)
    raise RuntimeError()

cdef bint test_2_embed_sattr_stext(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    t1 = xml.stag("tag1")
    t2 = t1.stag("tag2")
    t2.sattr("key", "value")
    t3 = t2.stag("tag3")
    t3.stext("description 3")
    t32 = t2.stag("tag3bis")
    t32.stext("description & 3bis")
    t32.sattr("x", "y")
    t32.stag("elem")
    t32.stag("elem")
    t32.stag("elem")

    result = xml.dump()
    expected = Str("<tag1>\n  <tag2 key=\"value\">\n    <tag3>description 3</tag3>\n    <tag3bis x=\"y\">description &amp; 3bis\n      <elem />\n      <elem />\n      <elem />\n    </tag3bis>\n  </tag2>\n</tag1>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_2_embed_sattr_stext", xml.max_depth)
    raise RuntimeError()

cdef bint test_2_embed_sattr_stext_tag_count(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result
    cdef int count, count_expected

    xml = cypXML()
    xml.set_max_depth(depth)
    t1 = xml.stag("tag1")
    t2 = t1.stag("tag2")
    t2.sattr("key", "value")
    t3 = t2.stag("tag3")
    t3.stext("description 3")
    t32 = t2.stag("tag3bis")
    t32.stext("description & 3bis")
    t32.sattr("x", "y")
    t32.stag("elem")
    t32.stag("elem")
    t32.stag("elem")
    count = xml.tag_count()

    result = xml.dump()
    count_expected = 7
    expected = Str("<tag1>\n  <tag2 key=\"value\">\n    <tag3>description 3</tag3>\n    <tag3bis x=\"y\">description &amp; 3bis\n      <elem />\n      <elem />\n      <elem />\n    </tag3bis>\n  </tag2>\n</tag1>\n")

    if result == expected and count == count_expected:
        return 1
    print("------------------------")
    print(f"tag count: {count}, expected: {count_expected}")
    print_err(expected, result, "test_2_embed_sattr_stext_tag_count", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_tail(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    p = xml.tag(Str("paragraph")).text(Str("begin part"))
    p.tag(Str("bold")).text(Str("bold part")).tail(Str("end part"))

    result = xml.dump()
    expected = Str("<paragraph>begin part\n  <bold>bold part</bold>end part\n</paragraph>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_tail", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_tail_escape(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    p = xml.tag(Str("paragraph")).text(Str("begin part with <>"))
    p.tag(Str("bold")).text(Str("bold &, < and >")).tail(Str("end part &, < and >"))

    result = xml.dump()
    expected = Str("<paragraph>begin part with &lt;&gt;\n  <bold>bold &amp;, &lt; and &gt;</bold>end part &amp;, &lt; and &gt;\n</paragraph>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_tail_escape", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_tail_escape_multi(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    d = xml.tag(Str("document"))
    d.tag(Str("params")).attr(Str("a1"), Str("true")).attr(Str("a2"), Str("false"))
    p = d.tag(Str("paragraph")).text(Str("begin part &<>")).attr(Str("style"), Str("title<'1'>"))
    p.tag(Str("bold")).text(Str("bold part &<>")).tail(Str("middle part, & < and >"))
    u = p.tag(Str("underline")).attr(Str("size"), Str("2"))
    u.text(Str("underlined part")).tail(Str("end part, \"&\" < and >"))
    p = d.tag(Str("paragraph")).text(Str("p2 begin part &, < and >"))
    p.attr(Str("style"), Str("standard<\"1\">"))
    p.tag(Str("bold")).text(Str("p2 bold part &, < and >")).tail(Str("p2 end part &, < and >"))

    result = xml.dump()
    expected = Str("<document>\n  <params a1=\"true\" a2=\"false\" />\n  <paragraph style=\"title&lt;\'1\'&gt;\">begin part &amp;&lt;&gt;\n    <bold>bold part &amp;&lt;&gt;</bold>middle part, &amp; &lt; and &gt;\n    <underline size=\"2\">underlined part</underline>end part, \"&amp;\" &lt; and &gt;\n  </paragraph>\n  <paragraph style=\'standard&lt;\"1\"&gt;\'>p2 begin part &amp;, &lt; and &gt;\n    <bold>p2 bold part &amp;, &lt; and &gt;</bold>p2 end part &amp;, &lt; and &gt;\n  </paragraph>\n</document>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_tail_escape_multi", xml.max_depth)
    raise RuntimeError()

cdef bint test_1_stail_escape_multi_all_s(int depth=3):
    cdef cypXML xml
    cdef Str expected
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    d = xml.stag("document")
    d.stag("params").sattr("a1", "true").sattr("a2", "false")
    p = d.stag("paragraph").stext("begin part &<>").sattr("style", "title<'1'>")
    p.stag("bold").stext("bold part &<>").stail("middle part, & < and >")
    u = p.stag("underline").sattr("size", "2")
    u.stext("underlined part").stail("end part, \"&\" < and >")
    p = d.stag("paragraph").stext("p2 begin part &, < and >")
    p.sattr("style", "standard<\"1\">")
    p.stag("bold").stext("p2 bold part &, < and >").stail("p2 end part &, < and >")

    result = xml.dump()
    expected = Str("<document>\n  <params a1=\"true\" a2=\"false\" />\n  <paragraph style=\"title&lt;\'1\'&gt;\">begin part &amp;&lt;&gt;\n    <bold>bold part &amp;&lt;&gt;</bold>middle part, &amp; &lt; and &gt;\n    <underline size=\"2\">underlined part</underline>end part, \"&amp;\" &lt; and &gt;\n  </paragraph>\n  <paragraph style=\'standard&lt;\"1\"&gt;\'>p2 begin part &amp;, &lt; and &gt;\n    <bold>p2 bold part &amp;, &lt; and &gt;</bold>p2 end part &amp;, &lt; and &gt;\n  </paragraph>\n</document>\n")

    if result == expected:
        return 1
    print_err(expected, result, "test_1_stail_escape_multi_all_s", xml.max_depth)
    raise RuntimeError()

##############################################################################

cdef bint test_utf8_document(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.init_version(Str("1.0"))
    t = xml.tag(Str("test"))
    d = t.tag(Str("description"))
    d.text(Str("An animated fantasy film from 1978 based on the first half of J.R.R Tolkien’s Lord of the Rings novel. The film was mainly filmed using rotoscoping, meaning it was filmed in live action sequences with real actors and then each frame was individually animated."
    ))

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_utf8_document", xml.max_depth)
    raise RuntimeError()

cdef bint test_simple_document(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.init_version(Str("1.0"))
    t = xml.tag(Str("person"))
    t2 = t.tag(Str("name"))
    t2.text(Str("Bob"))
    t2 = t.tag(Str("city"))
    t2.text(Str("Qusqu"))

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_simple_document", xml.max_depth)
    raise RuntimeError()

cdef bint test_rootless_fragment(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.tag(Str("data")).attr(Str("value"), Str("Just some data"))

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_rootless_fragment", xml.max_depth)
    raise RuntimeError()

cdef bint test_nested_elements(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    feed = xml.tag(Str("feed")).attr(Str("xmlns"), Str("http://www.w3.org/2005/Atom"))
    feed.tag(Str("title")).text(Str("Example Feed"))
    feed.tag(Str("updated")).text(Str("2003-12-13T18:30:02Z"))
    a = feed.tag(Str("author"))
    a.tag(Str("name")).text(Str("John Doe"))
    feed.tag(Str("id")).text(Str("urn:uuid:60a76c80-d399-11d9-b93C-0003939e0af6"))
    e = feed.tag(Str("entry"))
    e.tag(Str("title")).text(Str("Atom-Powered Robots Run Amok"))
    e.tag(Str("id")).text(Str("urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a"))
    e.tag(Str("updated")).text(Str("2003-12-13T18:30:02Z"))
    e.tag(Str("summary")).text(Str("Some text."))

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_nested_elements", xml.max_depth)
    raise RuntimeError()

cdef bint test_namespaces(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    p = xml.tag(Str("parent")).attr(Str("xmlns:my"), Str("http://example.org/ns/"))
    p.tag(Str("my:child")).attr(Str("my:attr"), Str("foo"))

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_namespaces", xml.max_depth)
    raise RuntimeError()

cdef bint test_extended_syntax(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.set_indent_space(Str("    "))  # change indentation to 4 spaces
    doc = xml.tag(Str("doc"))
    e = doc.tag(Str("elem"))
    e.attr(Str("x"), Str("x")).attr(Str("y"), Str("y")).attr(Str("z"), Str("z"))
    e = doc.tag(Str("elem"))
    e.attr(Str("x"), Str("x")).attr(Str("y"), Str("y")).attr(Str("z"), Str("z"))
    e = doc.tag(Str("elem"))
    # order keep ok:
    e.attr(Str("z"), Str("z")).attr(Str("y"), Str("y")).attr(Str("x"), Str("x"))
    doc.tag(Str("container")).tag(Str("elem"))
    doc.tag(Str("container")).tag(Str("elem"))

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_extended_syntax", xml.max_depth)
    raise RuntimeError()

cdef bint test_content_escaping(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    doc = xml.tag(Str("doc"))
    i = doc.tag(Str("item")).attr(Str("some_attr"), Str("attribute&value>to<escape"))
    i.text(Str("Text&to<escape"))

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_content_escaping", xml.max_depth)
    raise RuntimeError()

cdef bint test_atom_feed(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.init_version(Str("1.0"))
    feed = xml.tag(Str("feed")).attr(Str("xmlns"), Str("http://www.w3.org/2005/Atom"))
    feed.tag(Str("title")).text(Str("Example Feed"))
    feed.tag(Str("link")).attr(Str("href"), Str("http://example.org/"))
    feed.tag(Str("updated")).text(Str("2003-12-13T18:30:02Z"))
    a = feed.tag(Str("author"))
    a.tag(Str("name")).text(Str("John Doe"))
    a.tag(Str("id")).text(Str("urn:uuid:60a76c80-d399-11d9-b93C-0003939e0af6"))
    a.tag(Str("title")).text(Str("Atom-Powered Robots Run Amok"))
    a.tag(Str("link")).attr(Str("href"), Str("http://example.org/2003/12/13/atom03"))
    a.tag(Str("id")).text(Str("urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a"))
    a.tag(Str("updated")).text(Str("2003-12-13T18:30:02Z"))
    a.tag(Str("summary")).text(Str("Some text."))
    c = a.tag(Str("content")).attr(Str("type"), Str("xhtml"))
    d = c.tag(Str("div")).attr(Str("xmlns"), Str("http://www.w3.org/1999/xhtml"))
    d.tag(Str("label")).attr(Str("for"), Str("some_field")).text(Str("Some label"))
    d.tag(Str("input")).attr(Str("type"), Str("text")).attr(Str("value"), Str(""))

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_atom_feed", xml.max_depth)
    raise RuntimeError()

cdef bint test_deep_tree(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.init_version(Str("1.0"))
    b = xml.stag("base")
    lv = b.stag("level-1").stag("level-2").stag("level-3").stag("level-4")
    lv = lv.stag("level-5").stag("level-6").stag("level-7").stag("level-8")
    lv.stext("Some deep info")
    lv = b.stag("level").stag("level").stag("level").stag("level")
    lv = lv.stag("level").stag("level").stag("level").stag("level")
    lv.stext("Another deep info")

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_deep_tree", xml.max_depth)
    raise RuntimeError()

cdef bint test_append_subtree(Str expected, int depth=3):
    cdef cypXML xml
    cdef Str result

    xml = cypXML()
    xml.set_max_depth(depth)
    xml.init_version(Str("1.0"))
    b = xml.stag("base")
    b.stag("element").stext("aaa")

    xml1 = cypXML()
    xml1.set_max_depth(depth)
    x1 = xml1.stag("subtree1").stag("element").stag("element")
    x1.stag("element").stext("ccc")

    b.append(xml1.dump())
    b.stag("element").stext("bbb")

    xml2 = cypXML()
    xml2.set_max_depth(depth)
    xml2.stag("subtree2").stag("element").stext("ddd")

    xml3 = cypXML()
    xml3.set_max_depth(depth)
    xml3.stag("subtree3").stag("element").stext("eee")

    xml.append(xml2.dump())
    xml.sappend("<string>sss</string>")
    xml.append(xml3.dump())

    result = xml.dump()

    if result == expected:
        return 1
    print_err(expected, result, "test_append_subtree", xml.max_depth)
    raise RuntimeError()

##############################################################################


def main():
    print("-------------------------------------")
    print("Test cypxml")
    test_min()
    test_min(0)  # no threads
    test_version()
    test_version(0)  # no threads
    test_sversion()
    test_insert_content()
    test_insert_content(0)
    test_insert_scontent()
    test_set_indent_space()
    test_set_indent_space(0)
    test_set_sindent_space()
    test_set_sindent_space(0)
    test_1_tag()
    test_1_tag(0)
    test_1_stag()
    test_2_tag()
    test_2_tag(0)
    test_3_tag()
    test_3_tag(0)
    test_3_tag_stag()
    test_1_tag_text()
    test_1_tag_text(0)
    test_1_tag_stext()
    test_2_tag_text()
    test_2_tag_text(0)
    test_2_tag_text_mod()
    test_2_tag_text_mod(0)
    test_1_attr()
    test_1_attr(0)
    test_1_sattr()
    test_1_attr_text_to_escape()
    test_1_attr_text_to_escape(0)
    test_1_embed()
    test_1_embed(0)
    test_1_embed_attr_text()
    test_1_embed_attr_text(0)
    test_2_embed_attr_text()
    test_2_embed_attr_text(0)
    test_2_embed_sattr_stext()
    test_2_embed_sattr_stext_tag_count()
    test_2_embed_sattr_stext_tag_count(0)
    test_1_tail()
    test_1_tail(0)
    test_1_tail_escape()
    test_1_tail_escape(0)
    test_1_tail_escape_multi()
    test_1_tail_escape_multi(0)
    test_1_stail_escape_multi_all_s()
    test_1_stail_escape_multi_all_s(0)
    test_utf8_document(test_content("utf8_document.xml"))
    test_utf8_document(test_content("utf8_document.xml"), 0)
    test_simple_document(test_content("simple_document.xml"))
    test_simple_document(test_content("simple_document.xml"), 0)
    test_rootless_fragment(test_content("rootless_fragment.xml"))
    test_rootless_fragment(test_content("rootless_fragment.xml"), 0)
    test_nested_elements(test_content("nested_elements.xml"))
    test_nested_elements(test_content("nested_elements.xml"), 0)
    test_namespaces(test_content("namespaces.xml"))
    test_namespaces(test_content("namespaces.xml"), 0)
    test_extended_syntax(test_content("extended_syntax.xml"))
    test_extended_syntax(test_content("extended_syntax.xml"), 0)
    test_content_escaping(test_content("content_escaping.xml"))
    test_content_escaping(test_content("content_escaping.xml"), 0)
    test_atom_feed(test_content("atom_feed.xml"))
    test_atom_feed(test_content("atom_feed.xml"), 0)
    test_deep_tree(test_content("deep_tree.xml"))
    test_deep_tree(test_content("deep_tree.xml"), 0)  # no threads
    test_deep_tree(test_content("deep_tree.xml"), 1)
    test_deep_tree(test_content("deep_tree.xml"), 2)
    test_deep_tree(test_content("deep_tree.xml"), 3)
    test_deep_tree(test_content("deep_tree.xml"), 4)
    test_deep_tree(test_content("deep_tree.xml"), 999)
    test_append_subtree(test_content("append_subtree.xml"))
    test_append_subtree(test_content("append_subtree.xml"), 0)
    print("Done.")
