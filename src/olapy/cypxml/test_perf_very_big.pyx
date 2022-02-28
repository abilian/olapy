from stdlib.string cimport Str
from stdlib.format cimport format
from libcythonplus.dict cimport cypdict
from libcythonplus.list cimport cyplist

from .cypxml cimport cypXML

from time import perf_counter


cdef cypXML gen_xml() nogil:
    xml = cypXML()
    xml.init_version(Str("1.0"))
    for world in range(8):
        w = xml.tag(Str("World")).attr(Str("zone"), format("{}", world))
        for geo in range(4):
            g = w.tag(Str("Geo")).attr(Str("zone"), format("{}", geo))
            for area in range(8):
                a = g.tag(Str("Area")).attr(Str("where"), format("{}", area))
                for city in range(40):
                    c = a.tag(Str("City"))
                    c.tag(Str("Name")).text(format("name of city {}", city))
                    c.tag(Str("Location")).text(format("location of city {}", city))
                    for item in range(50):
                        c.tag(format("item")).attr(Str("ref"), format("{}", item)).attr(Str("number"), Str("10")).attr(Str("date"), Str("2022-1-1"))

    return xml

##############################################################################

cdef void one_test(int cores, int depth, msg):
    cdef cypXML xml
    cdef Str result_str

    print("-------------------------------------")
    print(msg)

    t0 = perf_counter()
    xml = gen_xml()
    dt_generate = (perf_counter() - t0) * 1000

    t0 = perf_counter()
    with nogil:
        xml.set_max_depth(depth)
        xml.set_workers(cores)
        result_str = xml.dump()
    content = result_str.bytes().decode("utf8")
    dt_dump = (perf_counter() - t0) * 1000

    xml = gen_xml()
    count = xml.tag_count()

    print(f"Duration total (ms): {dt_dump+dt_generate:.0f}")
    print(f"  - dump xml (ms): {dt_dump:.0f}")
    print(f"  - gen xml  (ms): {dt_generate:.0f}")
    print(f"Depth_max: {depth}, workers: {cores}")
    print("-------------------------------------")
    print(f"Size (MB): {len(content)/(2**20):.2f}")
    print(f"nb of tags: {count}")
    print("...")
    print("\n".join(content.splitlines()[:3]))
    print("...")
    print("-------------------------------------")
    print()


def t1():
    one_test(0, 1, "1 - Test very big file - cythonplus cypXML, cores = auto")

def t2():
    one_test(2, 1, "2 - Test very big file - cythonplus cypXML")

def t3():
    one_test(0, 2, "3 - Test very big file - cythonplus cypXML, cores = auto")

def t4():
    one_test(2, 2, "4 - Test very big file - cythonplus cypXML")

def t5():
    one_test(0, 3, "5 - Test very big file - cythonplus cypXML, cores = auto")

def t6():
    one_test(4, 999, "6 - Test very big file - cythonplus cypXML")

def t7():
    one_test(0, 0, "7 - Test very big file - cythonplus cypXML, recursive (no thread)")
