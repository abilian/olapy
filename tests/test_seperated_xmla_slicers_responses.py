from __future__ import absolute_import, division, print_function
import xmlwitch

from olapy.core.mdx.executor.execute import MdxEngine
from olapy.core.services.xmla_execute_tools import XmlaExecuteTools
from tests.queries import query11, query12, query13, query14


def test_slicer_axis_query11():
    """
    One measure.
    :return:
    """
    executor = MdxEngine('sales')
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[Geography].[Geography]"):
                    xml.UName('[Geography].[Geography].[Continent].[America]')
                    xml.Caption('America')
                    xml.LName('[Geography].[Geography].[Continent]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Company].[Crazy Development]')
                    xml.Caption('Crazy Development')
                    xml.LName('[Product].[Product].[Company]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Time].[Time]"):
                    xml.UName('[Time].[Time].[Year].[2010]')
                    xml.Caption('2010')
                    xml.LName('[Time].[Time].[Year]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName('[Measures].[Amount]')
                    xml.Caption('Amount')
                    xml.LName('[Measures]')
                    xml.LNum('0')
                    xml.DisplayInfo('0')

    executor.mdx_query = query11
    xmla_tools = XmlaExecuteTools(executor, False)
    assert str(xml) == xmla_tools.generate_slicer_axis()


def test_slicer_axis_query12():
    """
    Many measure.
    :return:
    """
    executor = MdxEngine('sales')
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[Geography].[Geography]"):
                    xml.UName('[Geography].[Geography].[Continent].[America]')
                    xml.Caption('America')
                    xml.LName('[Geography].[Geography].[Continent]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Company].[Crazy Development]')
                    xml.Caption('Crazy Development')
                    xml.LName('[Product].[Product].[Company]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Time].[Time]"):
                    xml.UName('[Time].[Time].[Year].[2010]')
                    xml.Caption('2010')
                    xml.LName('[Time].[Time].[Year]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')

    executor.mdx_query = query12

    xmla_tools = XmlaExecuteTools(executor, False)

    assert str(xml) == xmla_tools.generate_slicer_axis()


def test_slicer_axis_query13():
    """
    Dimension without measures.
    :return:
    """
    executor = MdxEngine('sales')
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Company].[Crazy Development]')
                    xml.Caption('Crazy Development')
                    xml.LName('[Product].[Product].[Company]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Time].[Time]"):
                    xml.UName('[Time].[Time].[Year].[2010]')
                    xml.Caption('2010')
                    xml.LName('[Time].[Time].[Year]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName('[Measures].[Amount]')
                    xml.Caption('Amount')
                    xml.LName('[Measures]')
                    xml.LNum('0')
                    xml.DisplayInfo('0')

    executor.mdx_query = query13

    xmla_tools = XmlaExecuteTools(executor, False)

    assert str(xml) == xmla_tools.generate_slicer_axis()


def test_slicer_axis_query14():
    """
    Dimension with all measures.
    :return:
    """
    executor = MdxEngine('sales')
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[Product].[Product]"):
                    xml.UName(
                        '[Product].[Product].[Company].[Crazy Development]')
                    xml.Caption('Crazy Development')
                    xml.LName('[Product].[Product].[Company]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Time].[Time]"):
                    xml.UName('[Time].[Time].[Year].[2010]')
                    xml.Caption('2010')
                    xml.LName('[Time].[Time].[Year]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')

    executor.mdx_query = query14

    xmla_tools = XmlaExecuteTools(executor, False)

    assert str(xml) == xmla_tools.generate_slicer_axis()
