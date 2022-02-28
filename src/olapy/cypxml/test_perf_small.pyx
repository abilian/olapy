from stdlib.string cimport Str
from stdlib.format cimport format
from libcythonplus.dict cimport cypdict
from libcythonplus.list cimport cyplist

from .cypxml cimport cypXML

from time import perf_counter


cdef cypXML gen_xml(int ref) nogil:
    xml = cypXML()
    xml.init_version(Str("1.0"))
    for geo in range(4):
        g = xml.stag("Geo").attr(Str("zone"), format("{}", geo)).attr(Str("ref"), format("{}", ref))
        for area in range(4):
            a = g.tag(Str("Area")).attr(Str("where"), format("{}", area))
            for city in range(4):
                a.stag("City").text(format("name of city {}", city))
    return xml

##############################################################################

cdef void one_test(int cores, int depth, msg):
    cdef cypXML xml
    cdef Str result_str
    cdef int ref
    cdef cyplist[cypXML] lst_xml

    print("-------------------------------------")
    print(msg)

    lst_xml = cyplist[cypXML]()
    t0 = perf_counter()
    for ref in range(100):
        lst_xml.append(gen_xml(ref))
    dt_generate = (perf_counter() - t0) * 1000

    lst_content = []
    t0 = perf_counter()
    for xml in lst_xml:
        with nogil:
            xml.set_max_depth(depth)
            xml.set_workers(cores)
            result_str = xml.dump()
        lst_content.append(result_str.bytes().decode("utf8"))
    dt_dump = (perf_counter() - t0) * 1000

    xml = gen_xml(0)
    count = xml.tag_count()
    content = lst_content[0]

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


def main():
    one_test(0, 1, "1 - Small file (x100) - cythonplus cypXML, cores = auto")
    one_test(2, 1, "2 - Small file (x100) - cythonplus cypXML")
    one_test(0, 2, "3 - Small file (x100) - cythonplus cypXML, cores = auto")
    one_test(2, 2, "4 - Small file (x100) - cythonplus cypXML")
    one_test(8, 2, "5 - Small file (x100) - cythonplus cypXML")
    one_test(0, 3, "6 - Small file (x100) - cythonplus cypXML, cores = auto")
    one_test(2, 3, "7 - Small file (x100) - cythonplus cypXML")
    one_test(8, 3, "8 - Small file (x100) - cythonplus cypXML")
    one_test(4, 999, "9 - Small file (x100) - cythonplus cypXML")
    one_test(0, 0, "10 - Small file (x100) - cythonplus cypXML, recursive (no thread)")
