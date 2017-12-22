from __future__ import absolute_import, division, print_function, \
    unicode_literals

import xmlwitch
from olapy.core.services.xmla_execute_tools import XmlaExecuteTools
from tests.queries import query11, query12, query13, query14



def test_slicer_axis(executor):
    slicer_axis_query11(executor)
    slicer_axis_query12(executor)
    slicer_axis_query13(executor)
    slicer_axis_query14(executor)

def slicer_axis_query11(executor):
    """
    One measure.
    :return:
    """
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName('[geography].[geography].[continent].[America]')
                    xml.Caption('America')
                    xml.LName('[geography].[geography].[continent]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        '[product].[product].[company].[Crazy Development]')
                    xml.Caption('Crazy Development')
                    xml.LName('[product].[product].[company]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[time].[time]"):
                    xml.UName('[time].[time].[year].[2010]')
                    xml.Caption('2010')
                    xml.LName('[time].[time].[year]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[Measures]"):
                    xml.UName('[Measures].[amount]')
                    xml.Caption('amount')
                    xml.LName('[Measures]')
                    xml.LNum('0')
                    xml.DisplayInfo('0')

    executor.mdx_query = query11
    xmla_tools = XmlaExecuteTools(executor, False)
    assert str(xml) == xmla_tools.generate_slicer_axis()


def slicer_axis_query12(executor):
    """
    Many measure.
    :return:
    """
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[geography].[geography]"):
                    xml.UName('[geography].[geography].[continent].[America]')
                    xml.Caption('America')
                    xml.LName('[geography].[geography].[continent]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        '[product].[product].[company].[Crazy Development]')
                    xml.Caption('Crazy Development')
                    xml.LName('[product].[product].[company]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[time].[time]"):
                    xml.UName('[time].[time].[year].[2010]')
                    xml.Caption('2010')
                    xml.LName('[time].[time].[year]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')

    executor.mdx_query = query12

    xmla_tools = XmlaExecuteTools(executor, False)

    assert str(xml) == xmla_tools.generate_slicer_axis()


def slicer_axis_query13(executor):
    """
    Dimension without measures.
    :return:
    """
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        '[product].[product].[company].[Crazy Development]')
                    xml.Caption('Crazy Development')
                    xml.LName('[product].[product].[company]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[time].[time]"):
                    xml.UName('[time].[time].[year].[2010]')
                    xml.Caption('2010')
                    xml.LName('[time].[time].[year]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')

    executor.mdx_query = query13

    xmla_tools = XmlaExecuteTools(executor, False)

    assert str(xml) == xmla_tools.generate_slicer_axis()


def slicer_axis_query14(executor):
    """
    Dimension with all measures.
    :return:
    """
    xml = xmlwitch.Builder()
    with xml.Axis(name="SlicerAxis"):
        with xml.Tuples:
            with xml.Tuple:
                with xml.Member(Hierarchy="[product].[product]"):
                    xml.UName(
                        '[product].[product].[company].[Crazy Development]')
                    xml.Caption('Crazy Development')
                    xml.LName('[product].[product].[company]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')
                with xml.Member(Hierarchy="[time].[time]"):
                    xml.UName('[time].[time].[year].[2010]')
                    xml.Caption('2010')
                    xml.LName('[time].[time].[year]')
                    xml.LNum('0')
                    xml.DisplayInfo('2')

    executor.mdx_query = query14

    xmla_tools = XmlaExecuteTools(executor, False)

    assert str(xml) == xmla_tools.generate_slicer_axis()
