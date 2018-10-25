from __future__ import absolute_import, division, print_function, \
    unicode_literals

import pytest
import xmlwitch

from olapy.core.services.xmla_execute_request_handler import XmlaExecuteReqHandler

from .queries import query15

sqlalchemy = pytest.importorskip("sqlalchemy")


def test_xs0_axis_query15(executor):
    """
    select ()()()...
    """
    xml = xmlwitch.Builder()
    with xml.Cell(CellOrdinal=str(0)):
        xml.Value(str(8), **{'xsi:type': 'xsi:long'})
    with xml.Cell(CellOrdinal=str(1)):
        xml.Value(str(144), **{'xsi:type': 'xsi:long'})
    with xml.Cell(CellOrdinal=str(2)):
        xml.Value(str(3), **{'xsi:type': 'xsi:long'})
    with xml.Cell(CellOrdinal=str(3)):
        xml.Value(str(4), **{'xsi:type': 'xsi:long'})
    # with xml.Cell(CellOrdinal=str(4)):
    #     xml.Value(str(96), **{'xsi:type': 'xsi:long'})

    xmla_tools = XmlaExecuteReqHandler(executor, query15, False)

    assert str(xml) == xmla_tools.generate_cell_data()
