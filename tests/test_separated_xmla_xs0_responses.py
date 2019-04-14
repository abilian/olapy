from __future__ import absolute_import, division, print_function, \
    unicode_literals

import pytest
import xmlwitch

from olapy.core.services.xmla_execute_request_handler import \
    XmlaExecuteReqHandler

from .queries import query15

sqlalchemy = pytest.importorskip("sqlalchemy")


def test_xs0_axis_query15(executor):
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
