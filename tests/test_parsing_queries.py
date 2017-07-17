from __future__ import absolute_import, division, print_function

from olapy.core.mdx.executor.execute import MdxEngine
from tests.queries import CUBE, query1, where, query2, query3, query4, query5, query6

executer = MdxEngine(CUBE)


def test_parsing_query1():
    query_parts = executer.decorticate_query(query1)

    assert query_parts['all'] == [['Measures', 'Amount']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Measures', 'Amount']]

    query_parts = executer.decorticate_query(query1 + '\n' + where)

    assert query_parts['all'] == [['Measures', 'Amount'],
                                  ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == [['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['columns'] == [['Measures', 'Amount']]


def test_parsing_query2():
    query_parts = executer.decorticate_query(query2)

    assert query_parts['all'] == [['Geography', 'Economy', 'Partnership']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Economy', 'Partnership']]

    query_parts = executer.decorticate_query(query2 + '\n' + where)

    assert query_parts['all'] == [['Geography', 'Economy', 'Partnership'],
                                  ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == [['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['columns'] == [['Geography', 'Economy', 'Partnership']]


def test_parsing_query3():
    query_parts = executer.decorticate_query(query3)

    assert query_parts['all'] == [['Geography', 'Geo', 'Country'],
                                  ['Measures', 'Amount']]
    assert query_parts['rows'] == [['Measures', 'Amount']]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Geo', 'Country']]

    query_parts = executer.decorticate_query(query3 + '\n' + where)

    assert query_parts['all'] == [['Geography', 'Geo',
                                   'Country'], ['Measures', 'Amount'],
                                  ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == [['Measures', 'Amount']]
    assert query_parts['where'] == [['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['columns'] == [['Geography', 'Geo', 'Country']]


def test_parsing_query4():
    query_parts = executer.decorticate_query(query4)

    assert query_parts['all'] == [['Geography', 'Economy', 'Partnership'],
                                  ['Geography', 'Geo', 'Country']]
    assert query_parts['rows'] == [['Geography', 'Geo', 'Country']]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Economy', 'Partnership']]

    query_parts = executer.decorticate_query(query4 + '\n' + where)

    assert query_parts['all'] == [['Geography', 'Economy', 'Partnership'],
                                  ['Geography', 'Geo', 'Country'],
                                  ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == [['Geography', 'Geo', 'Country']]
    assert query_parts['where'] == [['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['columns'] == [['Geography', 'Economy', 'Partnership']]


def test_parsing_query5():
    query_parts = executer.decorticate_query(query5)

    assert query_parts['all'] == [[
        'Geography', 'Economy', 'Partnership', 'EU'
    ], ['Geography', 'Economy', 'Partnership', 'None'], [
        'Geography', 'Economy', 'Partnership', 'NAFTA'
    ], ['Product', 'Prod', 'Company', 'Crazy Development'], [
        'Product', 'Prod', 'Company', 'Company_test'
    ], ['Product', 'Prod', 'Company', 'test_Development']]

    assert query_parts['rows'] == [[
        'Product', 'Prod', 'Company', 'Crazy Development'
    ], ['Product', 'Prod', 'Company',
        'Company_test'], ['Product', 'Prod', 'Company', 'test_Development']]

    assert query_parts['where'] == []

    assert query_parts['columns'] == [[
        'Geography', 'Economy', 'Partnership', 'EU'
    ], ['Geography', 'Economy', 'Partnership',
        'None'], ['Geography', 'Economy', 'Partnership', 'NAFTA']]

    query_parts = executer.decorticate_query(query5 + '\n' + where)

    assert query_parts['all'] == [[
        'Geography', 'Economy', 'Partnership', 'EU'
    ], ['Geography', 'Economy', 'Partnership', 'None'], [
        'Geography', 'Economy', 'Partnership', 'NAFTA'
    ], ['Product', 'Prod', 'Company', 'Crazy Development'], [
        'Product', 'Prod', 'Company', 'Company_test'
    ], ['Product', 'Prod', 'Company', 'test_Development'],
                                  ['Time', 'Calendar', 'Day', 'May 12,2010']]

    assert query_parts['rows'] == [[
        'Product', 'Prod', 'Company', 'Crazy Development'
    ], ['Product', 'Prod', 'Company',
        'Company_test'], ['Product', 'Prod', 'Company', 'test_Development']]

    assert query_parts['where'] == [['Time', 'Calendar', 'Day', 'May 12,2010']]

    assert query_parts['columns'] == [[
        'Geography', 'Economy', 'Partnership', 'EU'
    ], ['Geography', 'Economy', 'Partnership',
        'None'], ['Geography', 'Economy', 'Partnership', 'NAFTA']]


def test_parsing_query6():

    # query generated by excel

    query_parts = executer.decorticate_query(query6)

    assert query_parts['all'] == [['Time', 'Time', 'Year'], [
        'Time', 'Time', 'Year', '2010'
    ], ['Time', 'Time', 'Quarter', '2010',
        'Q2 2010'], ['Time', 'Time', 'Month', '2010', 'Q2 2010', 'May 2010'],
                                  ['Measures', 'Amount']]

    assert query_parts['rows'] == []

    assert query_parts['where'] == [['Measures', 'Amount']]

    assert query_parts['columns'] == [['Time', 'Time', 'Year'], [
        'Time', 'Time', 'Year', '2010'
    ], ['Time', 'Time', 'Quarter', '2010',
        'Q2 2010'], ['Time', 'Time', 'Month', '2010', 'Q2 2010', 'May 2010']]

    query_parts = executer.decorticate_query(query6 + '\n' + where)

    assert query_parts['all'] == [['Time', 'Time', 'Year'], [
        'Time', 'Time', 'Year', '2010'
    ], ['Time', 'Time', 'Quarter', '2010',
        'Q2 2010'], ['Time', 'Time', 'Month', '2010', 'Q2 2010',
                     'May 2010'], ['Measures', 'Amount'],
                                  ['Time', 'Calendar', 'Day', 'May 12,2010']]

    assert query_parts['rows'] == []

    assert query_parts['where'] == [['Measures', 'Amount'],
                                    ['Time', 'Calendar', 'Day', 'May 12,2010']]

    assert query_parts['columns'] == [['Time', 'Time', 'Year'], [
        'Time', 'Time', 'Year', '2010'
    ], ['Time', 'Time', 'Quarter', '2010',
        'Q2 2010'], ['Time', 'Time', 'Month', '2010', 'Q2 2010', 'May 2010']]
