from __future__ import absolute_import, division, print_function

import pandas as pd
from pandas.util.testing import assert_frame_equal

from olapy.core.mdx.executor.execute import MdxEngine
from olapy.core.mdx.parser.parse import MdxParser

CUBE = 'sales'

query1 = "SELECT" \
         "{[Measures].[Amount]} ON COLUMNS " \
         "FROM [sales]"

query2 = """SELECT
         {[Geography].[Economy].[Partnership]} ON COLUMNS
         FROM [sales]"""

query3 = """SELECT
         {[Measures].[Amount]} on 0,
         non empty {[Geography].[Geo].[Country].members} ON COLUMNS
         FROM [sales]"""

query4 = """SELECT
         {[Geography].[Economy].[Partnership]} ON COLUMNS,
         non empty {[Geography].[Geo].[Country].members} on 1
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

where1 = "Where [Time].[Calendar].[Day].[May 12,2010]"
where2 = "Where[Product].[olapy].[Personal]"
where3 = "Where[Time].[Calendar].[Year].[2010]"
where4 = "Where [Measures].[Count]"
where5 = "where [Count]"

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

parser = MdxParser()
executer = MdxEngine(CUBE)


def test_parsing_query1():
    assert parser.parsing_mdx_query(
        'column', query=query1) == ['Measures', 'Amount']
    assert parser.parsing_mdx_query('cube', query=query1) == "sales"
    assert parser.parsing_mdx_query('row', query=query1) is None
    query1_where = query1 + '\n' + where1
    assert parser.parsing_mdx_query(
        'condition',
        query=query1_where) == [u'Time', u'Calendar', u'Day', u'May 12,2010']
    query2_where = query1 + '\n' + where2
    assert parser.parsing_mdx_query(
        'condition', query=query2_where) == [u'Product', u'olapy', u'Personal']
    query3_where = query1 + '\n' + where3
    assert parser.parsing_mdx_query(
        'condition',
        query=query3_where) == [u'Time', u'Calendar', u'Year', u'2010']


def test_parsing_query2():
    assert parser.parsing_mdx_query(
        'column', query=query2) == [u'Geography', u'Economy', u'Partnership']
    assert parser.parsing_mdx_query('cube', query=query2) == "sales"
    assert parser.parsing_mdx_query('row', query=query2) is None
    query1_where = query2 + '\n' + where1
    assert parser.parsing_mdx_query(
        'condition',
        query=query1_where) == [u'Time', u'Calendar', u'Day', u'May 12,2010']
    query2_where = query2 + '\n' + where2
    assert parser.parsing_mdx_query(
        'condition', query=query2_where) == [u'Product', u'olapy', u'Personal']
    query3_where = query2 + '\n' + where3
    assert parser.parsing_mdx_query(
        'condition',
        query=query3_where) == [u'Time', u'Calendar', u'Year', u'2010']


def test_parsing_query3():
    assert parser.parsing_mdx_query(
        'column', query=query3) == [u'Measures', u'Amount']
    assert parser.parsing_mdx_query('cube', query=query3) == "sales"
    assert parser.parsing_mdx_query(
        'row', query=query3) == [u'Geography', u'Geo', u'Country', u'members']
    query1_where = query3 + '\n' + where1
    assert parser.parsing_mdx_query(
        'condition',
        query=query1_where) == [u'Time', u'Calendar', u'Day', u'May 12,2010']
    query2_where = query3 + '\n' + where2
    assert parser.parsing_mdx_query(
        'condition', query=query2_where) == [u'Product', u'olapy', u'Personal']
    query3_where = query3 + '\n' + where3
    assert parser.parsing_mdx_query(
        'condition',
        query=query3_where) == [u'Time', u'Calendar', u'Year', u'2010']


def test_parsing_query4():
    query0 = query4
    assert parser.parsing_mdx_query(
        'column', query=query0) == [u'Geography', u'Economy', u'Partnership']
    assert parser.parsing_mdx_query('cube', query=query0) == "sales"
    assert parser.parsing_mdx_query(
        'row', query=query0) == [u'Geography', u'Geo', u'Country', u'members']
    query1_where = query0 + '\n' + where1
    assert parser.parsing_mdx_query(
        'condition',
        query=query1_where) == [u'Time', u'Calendar', u'Day', u'May 12,2010']
    query2_where = query0 + '\n' + where2
    assert parser.parsing_mdx_query(
        'condition', query=query2_where) == [u'Product', u'olapy', u'Personal']
    query3_where = query0 + '\n' + where3
    assert parser.parsing_mdx_query(
        'condition',
        query=query3_where) == [u'Time', u'Calendar', u'Year', u'2010']


def test_parsing_query5():
    query0 = query5
    assert parser.parsing_mdx_query(
        'column', query=query0) == [u'Geography', u'Economy', u'Country']
    assert parser.parsing_mdx_query('cube', query=query0) == "sales"
    assert parser.parsing_mdx_query(
        'row', query=query0) == [u'Geography', u'Geo', u'Country', u'members']
    query1_where = query0 + '\n' + where1
    assert parser.parsing_mdx_query(
        'condition',
        query=query1_where) == [u'Time', u'Calendar', u'Day', u'May 12,2010']
    query2_where = query0 + '\n' + where2
    assert parser.parsing_mdx_query(
        'condition', query=query2_where) == [u'Product', u'olapy', u'Personal']
    query3_where = query0 + '\n' + where3
    assert parser.parsing_mdx_query(
        'condition',
        query=query3_where) == [u'Time', u'Calendar', u'Year', u'2010']


def test_parsing_query6():
    query0 = query6
    assert parser.parsing_mdx_query(
        'column', query=query0) == [u'Geography', u'Economy', u'Partnership']
    assert parser.parsing_mdx_query('cube', query=query0) == "sales"
    assert parser.parsing_mdx_query(
        'row', query=query0) == [u'Product', u'Prod', u'Company']
    query1_where = query0 + '\n' + where1
    assert parser.parsing_mdx_query(
        'condition',
        query=query1_where) == [u'Time', u'Calendar', u'Day', u'May 12,2010']
    query2_where = query0 + '\n' + where2
    assert parser.parsing_mdx_query(
        'condition', query=query2_where) == [u'Product', u'olapy', u'Personal']
    query3_where = query0 + '\n' + where3
    assert parser.parsing_mdx_query(
        'condition',
        query=query3_where) == [u'Time', u'Calendar', u'Year', u'2010']


def test_execution_query1():
    executer.mdx_query = query1
    assert executer.execute_mdx()['result']['Amount'][0] == 1023
    executer.mdx_query = query11
    assert executer.execute_mdx()['result']['Amount'][3] == 1
    assert executer.execute_mdx()['result']['Amount'][4] == 2


def test_execution_query3():
    df = pd.DataFrame({'Continent': ['America', 'Europe'],
                       'Amount': [768, 255]}
                      ).groupby(['Continent']).sum()

    executer.mdx_query = query12
    assert assert_frame_equal(df, executer.execute_mdx()['result']) is None

    executer.mdx_query = query11

    assert list(executer.execute_mdx()['result']['Amount']) == [1023, 1023, 1023, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
