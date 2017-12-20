from __future__ import absolute_import, division, print_function

import os
import pandas as pd
import pytest
import sqlalchemy
from olapy.core.mdx.executor.execute import MdxEngine
from pandas.util.testing import assert_frame_equal
from tests.mysql_utils import create_insert
from tests.queries import query3, query6, query10

MdxEngine.source_type = ('csv', 'db')
CUBE = 'main'


# @pytest.mark.skipif("os.environ['DB_TEST'] != 'SQLITE_MEM'")
@pytest.fixture(scope='function')
def connect():
    """Returns a connection and a metadata object"""
    eng = sqlalchemy.create_engine("sqlite://")
    os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
    MdxEngine.engine = eng
    return eng


# @pytest.mark.skipif("os.environ['DB_TEST'] != 'SQLITE_MEM'")
# create tables in the postgres database
def test_create_tables(connect):
    create_insert(connect)


# @pytest.mark.skipif("os.environ['DB_TEST'] != 'SQLITE_MEM'")
@pytest.fixture(scope='module')
def executor():
    return MdxEngine(CUBE)


# @pytest.mark.skipif("os.environ['DB_TEST'] != 'SQLITE_MEM'")
def test_execution_query1(executor):
    df = executor.execute_mdx(query3)['result']
    test_df = pd.DataFrame({
        'Country': ['France', 'Spain', 'Switzerland', 'United States'],
        'Amount': [4, 3, 248, 768],
    }).groupby(['Country']).sum()
    assert assert_frame_equal(df, test_df) is None


# @pytest.mark.skipif("os.environ['DB_TEST'] != 'SQLITE_MEM'")
def test_execution_query2(executor):
    df = executor.execute_mdx(query6)['result']
    test_df = pd.DataFrame({
        'Year': [
            2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010,
            2010, 2010
        ],
        'Quarter': [
            -1, 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010',
            'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010',
            'Q2 2010'
        ],
        'Month': [
            -1, -1, 'May 2010', 'May 2010', 'May 2010', 'May 2010', 'May 2010',
            'May 2010', 'May 2010', 'May 2010', 'May 2010', 'May 2010',
            'May 2010'
        ],
        'Day': [
            -1, -1, -1, 'May 12,2010', 'May 13,2010', 'May 14,2010',
            'May 15,2010', 'May 16,2010', 'May 17,2010', 'May 18,2010',
            'May 19,2010', 'May 20,2010', 'May 21,2010'
        ],
        'Amount': [1023, 1023, 1023, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    }).groupby(['Year', 'Quarter', 'Month', 'Day']).sum()

    assert assert_frame_equal(df, test_df) is None


# @pytest.mark.skipif("os.environ['DB_TEST'] != 'SQLITE_MEM'")
def test_execution_query10(executor):
    df = executor.execute_mdx(query10)['result']
    test_df = pd.DataFrame({
        'Year': [2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010],
        'Quarter': [
            'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010',
            'Q2 2010', 'Q2 2010'
        ],
        'Month': [
            'May 2010', 'May 2010', 'May 2010', 'May 2010', 'May 2010',
            'May 2010', 'May 2010', 'May 2010'
        ],
        'Day': [
            'May 19,2010',
            'May 17,2010',
            'May 15,2010',
            'May 13,2010',
            'May 12,2010',
            'May 14,2010',
            'May 16,2010',
            'May 18,2010',
        ],
        'Continent': [
            'Europe', 'Europe', 'Europe', 'Europe', 'Europe', 'Europe',
            'Europe', 'Europe'
        ],
        'Count': [13, 65, 231, 841, 84, 2, 4, 64]
    }).groupby(
        ['Year', 'Quarter', 'Month', 'Day', 'Continent'], sort=False).sum()

    assert assert_frame_equal(df, test_df) is None
