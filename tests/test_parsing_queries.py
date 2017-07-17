from __future__ import absolute_import, division, print_function

from olapy.core.mdx.executor.execute import MdxEngine

CUBE = 'sales'

query1 = """SELECT
         {[Measures].[Amount]} ON COLUMNS
         FROM [sales]"""

query2 = """SELECT
         {[Geography].[Economy].[Partnership]} ON COLUMNS
         FROM [sales]"""

query3 = """SELECT
         non empty {[Geography].[Geo].[Country].members} ON COLUMNS,
         {[Measures].[Amount]} ON ROWS
         FROM [sales]"""

query4 = """SELECT
         {[Geography].[Economy].[Partnership]} ON COLUMNS,
         non empty {[Geography].[Geo].[Country].members} ON ROWS
         from [sales]"""

query5 = """select
         {[Geography].[Economy].[Country]} on 0,
         non empty {[Geography].[Geo].[Country].members} on 1
         from [sales]"""

query6 = """select
         {[Geography].[Economy].[Partnership]} on 0,
         {[Product].[Prod].[Company]} on 1
         from [sales]"""

query7 = """select
         {[Geography].[Economy].[Partnership].[EU]} on 0,
         {[Product].[Prod].[Company].[Crazy Development]} on 1
         from [sales]"""

query8 = """select
         {[Geography].[Economy].[Partnership].[EU],
         [Geography].[Economy].[Partnership].[None],
         [Geography].[Economy].[Partnership].[NAFTA]} on 0,
         {[Product].[Prod].[Company].[Crazy Development],
         [Product].[Prod].[Company].[Company_test],
         [Product].[Prod].[Company].[test_Development]} on 1
         from [sales]"""

query9 = """select
         {[Geography].[Economy].[Partnership].[EU],
         [Geography].[Economy].[Partnership].[None]} on 0
         from [sales]"""

query10 = """select
          {[Geography].[Geo].[Country].[France],
          [Geography].[Geo].[Country].[Spain]} on 0,
          non empty {[Measures].[Amount]} on 1
          from [sales]"""

where = "Where [Time].[Calendar].[Day].[May 12,2010]"

query11 = """
          SELECT NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{DrilldownMember({{
          [Time].[Time].[Year].Members}}, {
          [Time].[Time].[Year].[2010]})}}, {
          [Time].[Time].[Quarter].[2010].[Q2 2010]})}}, {
          [Time].[Time].[Month].[2010].[Q2 2010].[May 2010]}))) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
          ON COLUMNS
          FROM [sales] WHERE ([Measures].[Amount])
          CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
          """

query12 = """SELECT NON EMPTY Hierarchize(AddCalculatedMembers({
            [Geography].[Geography].[Continent].Members}))
            DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
            FROM [sales]
            WHERE ([Measures].[Amount])
            CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

executer = MdxEngine(CUBE)


def test_parsing_query1():
    query_parts = executer.decorticate_query(query1)

    assert query_parts['all'] == [['Measures', 'Amount']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Measures', 'Amount']]

    query_parts = executer.decorticate_query(query1 + '\n' + where)

    assert query_parts['all'] == [['Measures', 'Amount'], ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Measures', 'Amount']]


def test_parsing_query2():
    query_parts = executer.decorticate_query(query2)

    assert query_parts['all'] == [['Geography', 'Economy', 'Partnership']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Economy', 'Partnership']]

    query_parts = executer.decorticate_query(query2 + '\n' + where)

    assert query_parts['all'] == [['Geography', 'Economy', 'Partnership'], ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Economy', 'Partnership']]


def test_parsing_query3():
    query_parts = executer.decorticate_query(query3)

    assert query_parts['all'] == [['Geography', 'Geo', 'Country', ''], ['Measures', 'Amount']]
    assert query_parts['rows'] == [['Measures', 'Amount']]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Geo', 'Country', '']]

    query_parts = executer.decorticate_query(query3 + '\n' + where)

    assert query_parts['all'] == [['Geography', 'Geo', 'Country', ''], ['Measures', 'Amount'],
                                  ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == [['Measures', 'Amount']]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Geo', 'Country', '']]


def test_parsing_query4():
    query_parts = executer.decorticate_query(query4)

    assert query_parts['all'] == [['Geography', 'Economy', 'Partnership'], ['Geography', 'Geo', 'Country', '']]
    assert query_parts['rows'] == [['Geography', 'Geo', 'Country', '']]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Economy', 'Partnership']]

    query_parts = executer.decorticate_query(query4 + '\n' + where)

    assert query_parts['all'] == [['Geography', 'Economy', 'Partnership'], ['Geography', 'Geo', 'Country', ''],
                                  ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == [['Geography', 'Geo', 'Country', '']]
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Geography', 'Economy', 'Partnership']]
