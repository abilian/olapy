from __future__ import absolute_import, division, print_function, unicode_literals
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
    with xml.Axis(name="Axis0"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Licence].[Crazy Development].[olapy].[Personal]'
                    )
                    xml.Caption('Personal')
                    xml.LName('[Product].[Product].[Licence]')
                    xml.LNum('2')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Geography].[Geography]"):
                    xml.UName(
                        '[Geography].[Geography].[Country].[Europe].[Switzerland]'
                    )
                    xml.Caption('Switzerland')
                    xml.LName('[Geography].[Geography].[Country]')
                    xml.LNum('1')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName('[Measures].[Amount]')
                    xml.Caption('Amount')
                    xml.LName('[Measures]')
                    xml.LNum('0')
                    xml.DisplayInfo('0')
            with xml.Tuple:
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Licence].[Crazy Development].[olapy].[Corporate]'
                    )
                    xml.Caption('Corporate')
                    xml.LName('[Product].[Product].[Licence]')
                    xml.LNum('2')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Geography].[Geography]"):
                    xml.UName(
                        '[Geography].[Geography].[Country].[Europe].[Switzerland]'
                    )
                    xml.Caption('Switzerland')
                    xml.LName('[Geography].[Geography].[Country]')
                    xml.LNum('1')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName('[Measures].[Amount]')
                    xml.Caption('Amount')
                    xml.LName('[Measures]')
                    xml.LNum('0')
                    xml.DisplayInfo('0')
            with xml.Tuple:
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Licence].[Crazy Development].[olapy].[Personal]'
                    )
                    xml.Caption('Personal')
                    xml.LName('[Product].[Product].[Licence]')
                    xml.LNum('2')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Geography].[Geography]"):
                    xml.UName(
                        '[Geography].[Geography].[Country].[Europe].[Spain]')
                    xml.Caption('Spain')
                    xml.LName('[Geography].[Geography].[Country]')
                    xml.LNum('1')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName('[Measures].[Amount]')
                    xml.Caption('Amount')
                    xml.LName('[Measures]')
                    xml.LNum('0')
                    xml.DisplayInfo('0')
            with xml.Tuple:
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Licence].[Crazy Development].[olapy].[Personal]'
                    )
                    xml.Caption('Personal')
                    xml.LName('[Product].[Product].[Licence]')
                    xml.LNum('2')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Geography].[Geography]"):
                    xml.UName(
                        '[Geography].[Geography].[Country].[Europe].[France]')
                    xml.Caption('France')
                    xml.LName('[Geography].[Geography].[Country]')
                    xml.LNum('1')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName('[Measures].[Amount]')
                    xml.Caption('Amount')
                    xml.LName('[Measures]')
                    xml.LNum('0')
                    xml.DisplayInfo('0')
            with xml.Tuple:
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Licence].[Crazy Development].[olapy].[Partnership]'
                    )
                    xml.Caption('Partnership')
                    xml.LName('[Product].[Product].[Licence]')
                    xml.LNum('2')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Geography].[Geography]"):
                    xml.UName(
                        '[Geography].[Geography].[Country].[Europe].[Switzerland]'
                    )
                    xml.Caption('Switzerland')
                    xml.LName('[Geography].[Geography].[Country]')
                    xml.LNum('1')
                    xml.DisplayInfo('131076')
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName('[Measures].[Amount]')
                    xml.Caption('Amount')
                    xml.LName('[Measures]')
                    xml.LNum('0')
                    xml.DisplayInfo('0')

    executor.mdx_query = query15

    xmla_tools = XmlaExecuteTools(executor, False)

    assert str(xml) == xmla_tools.generate_xs0()
