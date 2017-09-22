from __future__ import absolute_import, division, print_function
import xmlwitch

from olapy.core.mdx.executor.execute import MdxEngine
from olapy.core.services.xmla_execute_tools import XmlaExecuteTools
from tests.queries import query15


def test_xs0_axis_query15():
    """
    select ()()()...
    :return:
    """
    executor = MdxEngine('sales')
    xml = xmlwitch.Builder()
    with xml.Cell(CellOrdinal=str(0)):
        xml.Value(str(8), **{'xsi:type': 'xsi:long'})
    with xml.Cell(CellOrdinal=str(1)):
        xml.Value(str(144), **{'xsi:type': 'xsi:long'})
    with xml.Cell(CellOrdinal=str(2)):
        xml.Value(str(3), **{'xsi:type': 'xsi:long'})
    with xml.Cell(CellOrdinal=str(3)):
        xml.Value(str(4), **{'xsi:type': 'xsi:long'})
    with xml.Cell(CellOrdinal=str(4)):
        xml.Value(str(96), **{'xsi:type': 'xsi:long'})

    executor.mdx_query = query15

    xmla_tools = XmlaExecuteTools(executor, False)

    assert str(xml) == xmla_tools.generate_cell_data()
