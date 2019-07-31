from __future__ import absolute_import, division, print_function, \
    unicode_literals

import pytest
import xmlwitch

from olapy.core.services.xmla_execute_request_handler import \
    XmlaExecuteReqHandler

from .queries import query11, query12, query14, query15

sqlalchemy = pytest.importorskip("sqlalchemy")


#
# Test slicer
#
def test_slicer_axis_query11(executor):
    """
    One measure.
    """
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName("[geography].[geography].[continent].[America]")
                    xml.Caption("America")
                    xml.LName("[geography].[geography].[continent]")
                    xml.LNum("0")
                    xml.DisplayInfo("2")
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName("[product].[product].[company].[Crazy Development]")
                    xml.Caption("Crazy Development")
                    xml.LName("[product].[product].[company]")
                    xml.LNum("0")
                    xml.DisplayInfo("2")
                with xml.Member(Hierarchy="[time].[time]"):
                    xml.UName("[time].[time].[year].[2010]")
                    xml.Caption("2010")
                    xml.LName("[time].[time].[year]")
                    xml.LNum("0")
                    xml.DisplayInfo("2")
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName("[Measures].[amount]")
                    xml.Caption("amount")
                    xml.LName("[Measures]")
                    xml.LNum("0")
                    xml.DisplayInfo("0")

    xmla_tools = XmlaExecuteReqHandler(executor, query11, False)
    assert str(xml) == xmla_tools.generate_slicer_axis()


def test_slicer_axis_query12(executor):
    """
    Many measures.
    """
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName("[geography].[geography].[continent].[America]")
                    xml.Caption("America")
                    xml.LName("[geography].[geography].[continent]")
                    xml.LNum("0")
                    xml.DisplayInfo("2")
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName("[product].[product].[company].[Crazy Development]")
                    xml.Caption("Crazy Development")
                    xml.LName("[product].[product].[company]")
                    xml.LNum("0")
                    xml.DisplayInfo("2")
                with xml.Member(Hierarchy="[time].[time]"):
                    xml.UName("[time].[time].[year].[2010]")
                    xml.Caption("2010")
                    xml.LName("[time].[time].[year]")
                    xml.LNum("0")
                    xml.DisplayInfo("2")

    xmla_tools = XmlaExecuteReqHandler(executor, query12, False)
    assert str(xml) == xmla_tools.generate_slicer_axis()


def test_slicer_axis_query14(executor):
    """
    Dimension with all measures.
    """
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName("[product].[product].[company].[Crazy Development]")
                    xml.Caption("Crazy Development")
                    xml.LName("[product].[product].[company]")
                    xml.LNum("0")
                    xml.DisplayInfo("2")
                with xml.Member(Hierarchy="[time].[time]"):
                    xml.UName("[time].[time].[year].[2010]")
                    xml.Caption("2010")
                    xml.LName("[time].[time].[year]")
                    xml.LNum("0")
                    xml.DisplayInfo("2")

    xmla_tools = XmlaExecuteReqHandler(executor, query14, False)
    assert str(xml) == xmla_tools.generate_slicer_axis()


#
# Test cell data
#
def test_query15_cell_data(executor):
    """
    select ()()()...
    """
    xml = xmlwitch.Builder()
    with xml.Cell(CellOrdinal=str(0)):
        xml.Value(str(8), **{"xsi:type": "xsi:long"})
    with xml.Cell(CellOrdinal=str(1)):
        xml.Value(str(144), **{"xsi:type": "xsi:long"})
    with xml.Cell(CellOrdinal=str(2)):
        xml.Value(str(3), **{"xsi:type": "xsi:long"})
    with xml.Cell(CellOrdinal=str(3)):
        xml.Value(str(4), **{"xsi:type": "xsi:long"})
    with xml.Cell(CellOrdinal=str(4)):
        xml.Value(str(96), **{"xsi:type": "xsi:long"})

    xmla_tools = XmlaExecuteReqHandler(executor, query15, False)

    assert str(xml) == xmla_tools.generate_cell_data()


#
# Test xs0 responses
#
def test_query15_xs0(executor):
    """
    select ()()()...
    """
    xml = xmlwitch.Builder()
    with xml.Axis(name="Axis0"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        "[product].[product].[Crazy Development].[olapy].[Personal]"
                    )
                    xml.Caption("Personal")
                    xml.LName("[product].[product].[Crazy Development]")
                    xml.LNum("1")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName("[geography].[geography].[Europe].[Switzerland]")
                    xml.Caption("Switzerland")
                    xml.LName("[geography].[geography].[Europe]")
                    xml.LNum("0")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName("[Measures].[amount]")
                    xml.Caption("amount")
                    xml.LName("[Measures]")
                    xml.LNum("0")
                    xml.DisplayInfo("0")
            with xml.Tuple:
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        "[product].[product].[Crazy Development].[olapy].[Corporate]"
                    )
                    xml.Caption("Corporate")
                    xml.LName("[product].[product].[Crazy Development]")
                    xml.LNum("1")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName("[geography].[geography].[Europe].[Switzerland]")
                    xml.Caption("Switzerland")
                    xml.LName("[geography].[geography].[Europe]")
                    xml.LNum("0")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName("[Measures].[amount]")
                    xml.Caption("amount")
                    xml.LName("[Measures]")
                    xml.LNum("0")
                    xml.DisplayInfo("0")
            with xml.Tuple:
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        "[product].[product].[Crazy Development].[olapy].[Personal]"
                    )
                    xml.Caption("Personal")
                    xml.LName("[product].[product].[Crazy Development]")
                    xml.LNum("1")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName("[geography].[geography].[Europe].[Spain]")
                    xml.Caption("Spain")
                    xml.LName("[geography].[geography].[Europe]")
                    xml.LNum("0")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName("[Measures].[amount]")
                    xml.Caption("amount")
                    xml.LName("[Measures]")
                    xml.LNum("0")
                    xml.DisplayInfo("0")
            with xml.Tuple:
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        "[product].[product].[Crazy Development].[olapy].[Personal]"
                    )
                    xml.Caption("Personal")
                    xml.LName("[product].[product].[Crazy Development]")
                    xml.LNum("1")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName("[geography].[geography].[Europe].[France]")
                    xml.Caption("France")
                    xml.LName("[geography].[geography].[Europe]")
                    xml.LNum("0")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName("[Measures].[amount]")
                    xml.Caption("amount")
                    xml.LName("[Measures]")
                    xml.LNum("0")
                    xml.DisplayInfo("0")
            with xml.Tuple:
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        "[product].[product].[Crazy Development].[olapy].[Partnership]"
                    )
                    xml.Caption("Partnership")
                    xml.LName("[product].[product].[Crazy Development]")
                    xml.LNum("1")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName("[geography].[geography].[Europe].[Switzerland]")
                    xml.Caption("Switzerland")
                    xml.LName("[geography].[geography].[Europe]")
                    xml.LNum("0")
                    xml.DisplayInfo("131076")
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName("[Measures].[amount]")
                    xml.Caption("amount")
                    xml.LName("[Measures]")
                    xml.LNum("0")
                    xml.DisplayInfo("0")

    xmla_tools = XmlaExecuteReqHandler(executor, query15, False)
    assert str(xml) == xmla_tools.generate_xs0()
