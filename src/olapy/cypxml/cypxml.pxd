from olapy.stdlib.string cimport Str
from olapy.stdlib._string cimport string
from olapy.stdlib.format cimport format
from libcythonplus.dict cimport cypdict
from libcythonplus.list cimport cyplist

from olapy.stdlib.xml_utils cimport escaped, quotedattr, nameprep, concate, indented

# from scheduler.scheduler cimport SequentialMailBox, NullResult, Scheduler
# Using BatchMailBox seems more fast:
from olapy.scheduler.scheduler cimport BatchMailBox, NullResult, Scheduler


cdef cypclass cypXML:
    """A basic cypclass providing XML document API - multicore implementation
    """
    int nb_workers
    int max_depth
    Str version
    cypElement root
    cyplist[Str] content
    Str indent_space
    cyplist[Str] chunks
    cyplist[Elem] rope

    __init__(self):
        # self.nb_workers = 0  # 0 => will use the detected nb of CPU
        self.nb_workers = 0
        self.max_depth = 3
        self.version = Str()
        self.indent_space = Str("  ")
        self.rope = cyplist[Elem]()
        # self.root = cypElement(Str(), Str("  "), Str(""), self.rope)
        self.root = NULL
        # self.root.children = cyplist[cypElement]()
        self.content = cyplist[Str]()
        self.chunks = NULL

    void set_max_depth(self, int depth):
        self.max_depth = depth

    void set_workers(self, int nb):
        self.nb_workers = nb

    void init_version(self, Str version):
        self.version = version.copy()

    void init_sversion(self, const char* version):
        self.version = Str(version).copy()

    void insert_content(self, Str content):
        self.content.append(content.copy())

    void insert_scontent(self, const char* content):
        self.content.append(Str(content).copy())

    void set_indent_space(self, Str indent_space):
        self.indent_space = indent_space.copy()

    void set_sindent_space(self, const char* indent_space):
        self.indent_space = Str(indent_space).copy()

    void append_header(self):
        if self.version.__len__() != 0:
            self.chunks.append(format(
                "<?xml version=\"{}\" encoding=\"utf-8\"?>\n",
                self.version
            ))

    Elem tag(self, Str name):
        cdef cypElement e

        if self.root is NULL:
            self.root = cypElement(Str(), self.indent_space, Str(), self.rope)
            self.root.child_space = Str()
        return self.root.tag(name)

    Elem stag(self, const char* name):
        return self.tag(Str(name))

    void append(self, Str content):
        cdef cypElement e

        if self.root is NULL:
            self.root = cypElement(Str(), self.indent_space, Str(), self.rope)
            self.root.child_space = Str()
        self.root.append(content)

    Elem sappend(self, const char* content):
        self.append(Str(content))

    int tag_count(self):
        cdef int nb

        if self.root is NULL:
            return 0
        return self.root.tag_count() - 1  # root is 0 tag

    Str dump(self):
        if self.max_depth == 0:
            return self.dump_mono_core()
        else:
            return self.dump_workers()

    Str dump_workers(self):
        """Generate XML as a Str() string. Multicore implementation.
        """
        cdef Str result, s_result
        cdef Str c_result
        cdef Str item
        cdef iso cypElement root
        cdef lock Scheduler scheduler
        cdef Elem link
        cdef cyplist[cypElement] children

        self.chunks = cyplist[Str]()
        self.append_header()
        for item in self.content:
            self.chunks.append(item)

        if self.root is NULL:
            result = concate(self.chunks)
            return result

        for link in self.rope:
            link.cut()
        self.rope.clear()
        # bug in garbage coll : that lines should not be necessary, the one
        # above should be enough:
        self.root.cut_rope()
        # self.rope = NULL

        scheduler = Scheduler(self.nb_workers)
        root = consume(self.root)

        root_dumper = activate(consume DumpRoot(
                                            scheduler,
                                            consume(root),
                                            self.max_depth,
                                            ))
        root_dumper.run(NULL)

        scheduler.finish()
        del scheduler

        c_root_dumper = consume(root_dumper)
        c_recorder = consume(c_root_dumper.recorder)
        c_result = Str(c_recorder.read_result())
        s_result = Str()
        s_result._str = c_result._str
        self.chunks.append(s_result)
        result = concate(self.chunks)
        return result

    Str dump_mono_core(self):
        """Generate XML as a Str() string. Monocore implementation.
        """
        cdef Str result
        cdef Str item
        cdef cypElement c

        self.chunks = cyplist[Str]()
        self.append_header()
        for item in self.content:
            self.chunks.append(item)
        if self.root is not NULL and self.root.children is not NULL:
            for c in self.root.children:
                self.chunks.append(c.dump_recursive())
        result = concate(self.chunks)
        return result


cdef cypclass DumpRoot activable:
    lock Scheduler scheduler
    active Recorder recorder
    iso cypElement element
    int max_depth

    __init__(
            self,
            lock Scheduler scheduler,
            iso cypElement root,
            int max_depth,
            ):
        self._active_result_class = NullResult
        self._active_queue_class = consume BatchMailBox(scheduler)
        self.scheduler = scheduler  # keep it for use with sub objects
        self.recorder = activate (consume Recorder(scheduler, NULL, 0))
        self.element = consume root
        self.max_depth = max_depth
        # we now size is > 0 for root element

    void run(self):
        cdef int index
        cdef int depth
        cdef iso cypElement elem

        self.recorder.store(NULL, 0, consume Str())
        self.recorder.store(NULL, 1, consume Str())

        depth = self.max_depth
        element = consume self.element
        self.element = NULL
        self.recorder.set_size(NULL, element.nb_children() + 2)
        index = 1  # can start at zero for DummpRoot: no begin and end
                   # but want to keep same Recoder class
        while 1:
            child = consume element.pop_child()
            if child is NULL:
                break
            index += 1
            dumper = <active Dumper> activate(consume Dumper(
                                                        self.scheduler,
                                                        self.recorder,
                                                        consume child,
                                                        index,
                                                        depth,
                                                        ))
            dumper.run(NULL)
        return


cdef cypclass Recorder activable:
    cypdict[int, Str] storage
    int size
    active Recorder parent_recorder
    iso Str result
    int parent_index

    __init__(
            self,
            lock Scheduler scheduler,
            active Recorder parent_recorder,
            int parent_index):
        self._active_result_class = NullResult
        self._active_queue_class = consume BatchMailBox(scheduler)
        self.storage = cypdict[int, Str]()
        self.size = 0
        self.parent_recorder = parent_recorder
        self.parent_index = parent_index
        self.result = NULL

    const char* read_result(self):
        cdef const char* final

        if self.result is NULL:
            final = Str("").bytes()
        else:
            final = self.result.bytes()
        return final

    void check_completion(self):
        cdef cyplist[Str] lst
        cdef int i
        cdef Str result

        if <int> self.storage.__len__() == self.size:
            lst = cyplist[Str]()
            lst.append(<Str> self.storage[0])  # begin
            if self.size >= 2:
                for i in range(2, self.size):
                    lst.append(<Str> self.storage[i])
                lst.append(<Str> self.storage[1])  # end
            result = concate(lst)
            lst = NULL
            if self.parent_recorder is NULL:
                self.result = consume result
            else:
                self.parent_recorder.store(
                                NULL,
                                self.parent_index,
                                consume(result)
                                )
        return

    void store(self, int key, iso Str value):
        cdef Str s

        s = Str()
        s._str = value._str
        self.storage[key] = s
        if self.size > 0:
            self.check_completion()
        return

    void set_size(self, int size):
        self.size = size
        if self.size > 0:
            self.check_completion()
        return


cdef cypclass Dumper activable:
    lock Scheduler scheduler
    active Recorder parent_recorder

    iso cypElement element
    int index
    int depth

    __init__(
            self,
            lock Scheduler scheduler,
            active Recorder parent_recorder,
            iso cypElement element,
            int index,
            int depth):
        self._active_result_class = NullResult
        self._active_queue_class = consume BatchMailBox(scheduler)
        self.scheduler = scheduler  # keep it for use with sub objects
        self.parent_recorder = parent_recorder
        self.element = consume element
        self.index = index
        self.depth = depth

    void request_children(self):
        cdef active Recorder recorder
        cdef int index

        recorder = activate (consume Recorder(
                                        scheduler,
                                        parent_recorder,
                                        self.index))
        recorder.set_size(NULL, self.element.nb_children() + 2)
        recorder.store(
                        NULL,
                        0,
                        consume(self.element.dump_begin())
                        )
        recorder.store(
                        NULL,
                        1,
                        consume(self.element.dump_end())
                        )
        index = 1
        while 1:
            child = consume self.element.pop_child()
            if child is NULL:
                break
            index += 1
            dumper = <active Dumper> activate(consume Dumper(
                                                        self.scheduler,
                                                        recorder,
                                                        consume child,
                                                        index,
                                                        self.depth,
                                                        ))
            dumper.run(NULL)
        return

    void run(self):
        self.depth -= 1
        if self.depth == 0:
            self.parent_recorder.store(
                        NULL,
                        self.index,
                        consume(self.element.dump_recursive())
                        )
        else:
            if self.element.nb_children() > 0:
                self.request_children()
            else:
                self.parent_recorder.store(
                            NULL,
                            self.index,
                            consume(self.element.dump_leaf())
                            )
        return


cdef cypclass cypElement:
    """A basic cypclass providing XML tag definition
    """
    Str name
    cyplist[cypElement] children
    cyplist[Str] attributes
    cyplist[Str] content
    cyplist[Str] content_tail
    Str indent_space
    Str current_space
    Str child_space
    cyplist[Elem] rope

    __init__(
            self,
            Str name,
            Str indent_space,
            Str current_space,
            cyplist[Elem] rope):
        self.name = nameprep(name)
        self.children = NULL
        self.attributes = NULL
        self.content = NULL
        self.content_tail = NULL
        self.indent_space = indent_space.copy()
        self.current_space = current_space.copy()
        self.child_space = NULL
        self.rope = rope

    Str dump_leaf(self):
        cdef Str result

        if self.content is not NULL and self.content.__len__() > 0:
            if self.content_tail is not NULL and self.content_tail.__len__() > 0:
                if self.attributes is not NULL and self.attributes.__len__() > 0:
                    result = format(
                        "{}<{}{}>{}</{}>{}\n",
                        self.current_space,
                        self.name,
                        concate(self.attributes),
                        concate(self.content),
                        self.name,
                        concate(self.content_tail),
                    )
                else:
                    result = format(
                        "{}<{}>{}</{}>{}\n",
                        self.current_space,
                        self.name,
                        concate(self.content),
                        self.name,
                        concate(self.content_tail),
                    )
            else:
                if self.attributes is not NULL and self.attributes.__len__() > 0:
                    result = format(
                        "{}<{}{}>{}</{}>\n",
                        self.current_space,
                        self.name,
                        concate(self.attributes),
                        concate(self.content),
                        self.name,
                    )
                else:
                    if self.name.__len__() == 0:
                        # it's an appended string, not a xml element
                        result = indented(self.content[0], self.current_space)
                    else:
                        result = format(
                            "{}<{}>{}</{}>\n",
                            self.current_space,
                            self.name,
                            concate(self.content),
                            self.name,
                        )
        else:
            if self.content_tail is not NULL and self.content_tail.__len__() > 0:
                if self.attributes is not NULL and self.attributes.__len__() > 0:
                    result = format(
                        "{}<{}{} />{}\n",
                        self.current_space,
                        self.name,
                        concate(self.attributes),
                        concate(self.content_tail),
                    )
                else:
                    result = format(
                        "{}<{} />{}\n",
                        self.current_space,
                        self.name,
                        concate(self.content_tail),
                    )
            else:
                if self.attributes is not NULL and self.attributes.__len__() > 0:
                    result = format(
                        "{}<{}{} />\n",
                        self.current_space,
                        self.name,
                        concate(self.attributes),
                    )
                else:
                    result = format(
                        "{}<{} />\n",
                        self.current_space,
                        self.name,
                    )
        return result

    Str dump_begin(self):
        cdef Str result

        if self.content is not NULL and self.content.__len__() > 0:
            if self.attributes is not NULL and self.attributes.__len__() > 0:
                result = format(
                    "{}<{}{}>{}\n",
                    self.current_space,
                    self.name,
                    concate(self.attributes),
                    concate(self.content),
                )
            else:
                result = format(
                    "{}<{}>{}\n",
                    self.current_space,
                    self.name,
                    concate(self.content),
                )
        else:
            if self.attributes is not NULL and self.attributes.__len__() > 0:
                result = format(
                    "{}<{}{}>\n",
                    self.current_space,
                    self.name,
                    concate(self.attributes),
                )
            else:
                result = format(
                    "{}<{}>\n",
                    self.current_space,
                    self.name,
                )
        return result

    Str dump_end(self):
        cdef Str result

        if self.content_tail is not NULL and self.content_tail.__len__() > 0:
            result = format(
                        "{}</{}>{}\n",
                        self.current_space,
                        self.name,
                        concate(self.content_tail),
                        )
        else:
            result = format(
                        "{}</{}>\n",
                        self.current_space,
                        self.name,
                        )
        return result

    Str dump_recursive(self):
        cdef cypElement c
        cdef cyplist[Str] child_dump
        cdef Str result

        if self.children is NULL or self.children.__len__() == 0:
            return self.dump_leaf()
        child_dump = cyplist[Str]()
        for c in self.children:
            child_dump.append(c.dump_recursive())
        if self.content is not NULL and self.content.__len__() > 0:
            if self.content_tail is not NULL and self.content_tail.__len__() > 0:
                if self.attributes is not NULL and self.attributes.__len__() > 0:
                    result = format(
                        "{}<{}{}>{}\n{}{}</{}>{}\n",
                        self.current_space,
                        self.name,
                        concate(self.attributes),
                        concate(self.content),
                        concate(child_dump),
                        self.current_space,
                        self.name,
                        concate(self.content_tail),
                    )
                else:
                    result = format(
                        "{}<{}>{}\n{}{}</{}>{}\n",
                        self.current_space,
                        self.name,
                        concate(self.content),
                        concate(child_dump),
                        self.current_space,
                        self.name,
                        concate(self.content_tail),
                    )
            else:
                if self.attributes is not NULL and self.attributes.__len__() > 0:
                    result = format(
                        "{}<{}{}>{}\n{}{}</{}>\n",
                        self.current_space,
                        self.name,
                        concate(self.attributes),
                        concate(self.content),
                        concate(child_dump),
                        self.current_space,
                        self.name,
                    )
                else:
                    result = format(
                        "{}<{}>{}\n{}{}</{}>\n",
                        self.current_space,
                        self.name,
                        concate(self.content),
                        concate(child_dump),
                        self.current_space,
                        self.name,
                    )
        else:
            if self.content_tail is not NULL and self.content_tail.__len__() > 0:
                if self.attributes is not NULL and self.attributes.__len__() > 0:
                    result = format(
                        "{}<{}{}>\n{}{}</{}>{}\n",
                        self.current_space,
                        self.name,
                        concate(self.attributes),
                        concate(child_dump),
                        self.current_space,
                        self.name,
                        concate(self.content_tail),
                    )
                else:
                    result = format(
                        "{}<{}>\n{}{}</{}>{}\n",
                        self.current_space,
                        self.name,
                        concate(child_dump),
                        self.current_space,
                        self.name,
                        concate(self.content_tail),
                    )
            else:
                if self.attributes is not NULL and self.attributes.__len__() > 0:
                    result = format(
                        "{}<{}{}>\n{}{}</{}>\n",
                        self.current_space,
                        self.name,
                        concate(self.attributes),
                        concate(child_dump),
                        self.current_space,
                        self.name,
                    )
                else:
                    result = format(
                        "{}<{}>\n{}{}</{}>\n",
                        self.current_space,
                        self.name,
                        concate(child_dump),
                        self.current_space,
                        self.name,
                    )
        return result

    cyplist[cypElement] pop_children(self):
        cdef cyplist[cypElement] clist

        clist = self.children
        self.children = NULL
        return clist

    cypElement pop_child(self):
        cdef cypElement e

        if self.children is NULL or self.children.__len__() == 0:
            return NULL
        e = self.children[0]
        del self.children[0]
        return e

    int nb_children(self):
        cdef int nb

        if self.children is NULL:
            nb = 0
        else:
            nb = <int> self.children.__len__()
        return nb

    int tag_count(self):
        cdef int nb

        nb = 1
        if self.children is not NULL:
            for c in self.children:
                nb += c.tag_count()
        return nb

    void cut_rope(self):
        self.rope = NULL
        if self.children is not NULL:
            for c in self.children:
                c.cut_rope()

    Elem tag(self, Str name):
        """Append an element as child of current element

        Return the added element
        """
        cdef cypElement e

        if self.child_space is NULL:
            self.child_space = format(
                                        "{}{}",
                                        self.current_space,
                                        self.indent_space,
                                    )

        e = cypElement(name, self.indent_space, self.child_space, self.rope)
        if self.children is NULL:
            self.children = cyplist[cypElement]()
        self.children.append(e)
        link = Elem(e)
        self.rope.append(link)
        return link

    void append(self, Str content):
        """Append an arbitrary string as child of current element. The string
        can be the dump() of another cypxml() instance.

        Return the root element (not the added string)
        """
        cdef cypElement e

        if content is NULL or content.__len__() == 0:
            return

        if self.child_space is NULL:
            self.child_space = format(
                                        "{}{}",
                                        self.current_space,
                                        self.indent_space,
                                    )

        # the null Str() indicates that this is not a regular element, but an appended string
        e = cypElement(Str(), self.indent_space, self.child_space, self.rope)
        e._set_content(content)
        if self.children is NULL:
            self.children = cyplist[cypElement]()
        self.children.append(e)

    void _set_content(self, Str content):
        self.content = cyplist[Str]()
        self.content.append(content)

    void attr(self, Str key, Str value):
        """Append an attribute to current element

        Return the element (self, not the added attribute)
        """
        if self.attributes is NULL:
            self.attributes = cyplist[Str]()
        self.attributes.append(format(
            " {}={}",
            nameprep(key),
            quotedattr(value, NULL)
        ))

    void text(self, Str txt):
        """Append a string to text content of current element.

        Return the element (self, not the added text)
        """
        if self.content is NULL:
            self.content = cyplist[Str]()
        self.content.append(escaped(txt, NULL))

    void tail(self, Str txt):
        """Append a string to tail content of current element. (The tail is
        the text after the element's end tag, but before the next sibling
        element's start tag).

        Return the element (self, not the added text)
        """
        if self.content_tail is NULL:
            self.content_tail = cyplist[Str]()
        self.content_tail.append(escaped(txt, NULL))


cdef cypclass Elem:
    cypElement target

    __init__(self, cypElement target):
        self.target = target

    Elem tag(self, Str name):
        return self.target.tag(name)

    Elem stag(self, const char * name):
        return self.target.tag(Str(name))

    Elem append(self, Str content):
        self.target.append(content)
        return self

    Elem sappend(self, const char * content):
        self.target.append(Str(content))

    Elem attr(self, Str key, Str value):
        self.target.attr(key, value)
        return self

    Elem sattr(self, const char * key, const char * value):
        self.target.attr(Str(key), Str(value))
        return self

    Elem text(self, Str txt):
        self.target.text(txt)
        return self

    Elem stext(self, const char * txt):
        self.target.text(Str(txt))
        return self

    Elem tail(self, Str txt):
        self.target.tail(txt)
        return self

    Elem stail(self, const char * txt):
        self.target.tail(Str(txt))
        return self

    Elem cut(self):
        self.target = NULL


cdef Str to_str(byte_or_string)
