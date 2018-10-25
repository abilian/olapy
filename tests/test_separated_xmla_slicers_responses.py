from __future__ import absolute_import, division, print_function, \
    unicode_literals

import pytest
import xmlwitch

from olapy.core.services.xmla_execute_request_handler import XmlaExecuteReqHandler

from .queries import query11, query12, query14

sqlalchemy = pytest.importorskip("sqlalchemy")


def test_slicer_axis_query11(executor):
    """
    One measure.
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

    xmla_tools = XmlaExecuteReqHandler(executor, query14, False)
    assert str(xml) == xmla_tools.generate_slicer_axis()
