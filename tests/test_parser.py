from __future__ import absolute_import, division, print_function, \
    unicode_literals

from pytest import fixture

from olapy.core.mdx.parser.parse import Parser

from .queries import query1, query2, query3, query4, query5, query6, where


@fixture(scope="module")
def parser():
    return Parser()


def test_parsing_query1(parser):
    query_parts = parser.decorticate_query(query1)
    assert query_parts['all'] == [['Measures', 'amount']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Measures', 'amount']]

    query_parts = parser.decorticate_query(query1 + '\n' + where)
    assert query_parts['all'] == [
        ['Measures', 'amount'],
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['rows'] == []
    assert query_parts['where'] == [
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['columns'] == [
        ['Measures', 'amount'],
    ]


def test_parsing_query2(parser):
    query_parts = parser.decorticate_query(query2)
    assert query_parts['all'] == [
        ['geography', 'economy', 'partnership'],
    ]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [
        ['geography', 'economy', 'partnership'],
    ]

    query_parts = parser.decorticate_query(query2 + '\n' + where)
    assert query_parts['all'] == [
        ['geography', 'economy', 'partnership'],
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['rows'] == []
    assert query_parts['where'] == [
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['columns'] == [
        ['geography', 'economy', 'partnership'],
    ]


def test_parsing_query3(parser):
    query_parts = parser.decorticate_query(query3)
    assert query_parts['all'] == [
        ['geography', 'geo', 'country'],
        ['Measures', 'amount'],
    ]
    assert query_parts['rows'] == [
        ['Measures', 'amount'],
    ]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [
        ['geography', 'geo', 'country'],
    ]

    query_parts = parser.decorticate_query(query3 + '\n' + where)
    assert query_parts['all'] == [
        ['geography', 'geo', 'country'],
        ['Measures', 'amount'],
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['rows'] == [
        ['Measures', 'amount'],
    ]
    assert query_parts['where'] == [
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['columns'] == [
        ['geography', 'geo', 'country'],
    ]


def test_parsing_query4(parser):
    query_parts = parser.decorticate_query(query4)
    assert query_parts['all'] == [
        ['geography', 'economy', 'partnership'],
        ['geography', 'geo', 'country'],
    ]
    assert query_parts['rows'] == [
        ['geography', 'geo', 'country'],
    ]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [
        ['geography', 'economy', 'partnership'],
    ]

    query_parts = parser.decorticate_query(query4 + '\n' + where)
    assert query_parts['all'] == [
        ['geography', 'economy', 'partnership'],
        ['geography', 'geo', 'country'],
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['rows'] == [
        ['geography', 'geo', 'country'],
    ]
    assert query_parts['where'] == [
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['columns'] == [
        ['geography', 'economy', 'partnership'],
    ]


def test_parsing_query5(parser):
    query_parts = parser.decorticate_query(query5)
    assert query_parts['all'] == [
        ['geography', 'economy', 'partnership', 'EU'],
        ['geography', 'economy', 'partnership', 'None'],
        ['geography', 'economy', 'partnership', 'NAFTA'],
        ['product', 'prod', 'company', 'Crazy Development'],
        ['product', 'prod', 'company', 'company_test'],
        ['product', 'prod', 'company', 'test_Development'],
    ]
    assert query_parts['rows'] == [
        ['product', 'prod', 'company', 'Crazy Development'],
        ['product', 'prod', 'company', 'company_test'],
        ['product', 'prod', 'company', 'test_Development'],
    ]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [
        ['geography', 'economy', 'partnership', 'EU'],
        ['geography', 'economy', 'partnership', 'None'],
        ['geography', 'economy', 'partnership', 'NAFTA'],
    ]

    query_parts = parser.decorticate_query(query5 + '\n' + where)
    assert query_parts['all'] == [
        ['geography', 'economy', 'partnership', 'EU'],
        ['geography', 'economy', 'partnership', 'None'],
        ['geography', 'economy', 'partnership', 'NAFTA'],
        ['product', 'prod', 'company', 'Crazy Development'],
        ['product', 'prod', 'company', 'company_test'],
        ['product', 'prod', 'company', 'test_Development'],
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['rows'] == [
        ['product', 'prod', 'company', 'Crazy Development'],
        ['product', 'prod', 'company', 'company_test'],
        ['product', 'prod', 'company', 'test_Development'],
    ]
    assert query_parts['where'] == [
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['columns'] == [
        ['geography', 'economy', 'partnership', 'EU'],
        ['geography', 'economy', 'partnership', 'None'],
        ['geography', 'economy', 'partnership', 'NAFTA'],
    ]


def test_parsing_query6(parser):
    # query generated by excel

    query_parts = parser.decorticate_query(query6)
    assert query_parts['all'] == [
        ['time', 'time', 'year'],
        ['time', 'time', 'year', '2010'],
        ['time', 'time', 'quarter', '2010', 'Q2 2010'],
        ['time', 'time', 'month', '2010', 'Q2 2010', 'May 2010'],
        ['Measures', 'amount'],
    ]
    assert query_parts['rows'] == []
    assert query_parts['where'] == [['Measures', 'amount']]
    assert query_parts['columns'] == [
        ['time', 'time', 'year'],
        ['time', 'time', 'year', '2010'],
        ['time', 'time', 'quarter', '2010', 'Q2 2010'],
        ['time', 'time', 'month', '2010', 'Q2 2010', 'May 2010'],
    ]

    query_parts = parser.decorticate_query(query6 + '\n' + where)
    assert query_parts['all'] == [
        ['time', 'time', 'year'],
        ['time', 'time', 'year', '2010'],
        ['time', 'time', 'quarter', '2010', 'Q2 2010'],
        ['time', 'time', 'month', '2010', 'Q2 2010', 'May 2010'],
        ['Measures', 'amount'],
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['rows'] == []
    assert query_parts['where'] == [
        ['Measures', 'amount'],
        ['time', 'calendar', 'day', 'May 12,2010'],
    ]
    assert query_parts['columns'] == [
        ['time', 'time', 'year'],
        ['time', 'time', 'year', '2010'],
        ['time', 'time', 'quarter', '2010', 'Q2 2010'],
        ['time', 'time', 'month', '2010', 'Q2 2010', 'May 2010'],
    ]
