from __future__ import absolute_import, division, print_function

import pandas as pd
import pytest
from pandas.util.testing import assert_frame_equal

from .queries import query3, query6, query10


@pytest.mark.skipif(
    "'DB_TEST' not in os.environ or os.environ['DB_TEST'] != 'SQLITE' or 'SQLITE_URI' not in os.environ"
)
@pytest.mark.parametrize('executor', [['SQLITE_URI']], indirect=True)
def test_mysql_execution_queries(executor):
    check_execution_query1(executor)
    check_execution_query2(executor)
    check_execution_query10(executor)


def check_execution_query1(executor_sqlite):
    df = executor_sqlite.execute_mdx(query3)['result']
    test_df = pd.DataFrame({
        'country': ['France', 'Spain', 'Switzerland', 'United States'],
        'amount': [4, 3, 248, 768],
    }).groupby(['country']).sum()
    assert_frame_equal(df, test_df)


def check_execution_query2(executor_sqlite):
    df = executor_sqlite.execute_mdx(query6)['result']
    test_df = pd.DataFrame({
        'year': [
            2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010,
            2010, 2010
        ],
        'quarter': [
            -1, 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010',
            'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010',
            'Q2 2010'
        ],
        'month': [
            -1, -1, 'May 2010', 'May 2010', 'May 2010', 'May 2010', 'May 2010',
            'May 2010', 'May 2010', 'May 2010', 'May 2010', 'May 2010',
            'May 2010'
        ],
        'day': [
            -1, -1, -1, 'May 12,2010', 'May 13,2010', 'May 14,2010',
            'May 15,2010', 'May 16,2010', 'May 17,2010', 'May 18,2010',
            'May 19,2010', 'May 20,2010', 'May 21,2010'
        ],
        'amount': [1023, 1023, 1023, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    }).groupby(['year', 'quarter', 'month', 'day']).sum()

    assert_frame_equal(df, test_df)


def check_execution_query10(executor_sqlite):
    df = executor_sqlite.execute_mdx(query10)['result']
    test_df = pd.DataFrame({
        'year': [2010, 2010, 2010, 2010, 2010, 2010, 2010, 2010],
        'quarter': [
            'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010', 'Q2 2010',
            'Q2 2010', 'Q2 2010'
        ],
        'month': [
            'May 2010', 'May 2010', 'May 2010', 'May 2010', 'May 2010',
            'May 2010', 'May 2010', 'May 2010'
        ],
        'day': [
            'May 19,2010',
            'May 17,2010',
            'May 15,2010',
            'May 13,2010',
            'May 12,2010',
            'May 14,2010',
            'May 16,2010',
            'May 18,2010',
        ],
        'continent': [
            'Europe', 'Europe', 'Europe', 'Europe', 'Europe', 'Europe',
            'Europe', 'Europe'
        ],
        'count': [13, 65, 231, 841, 84, 2, 4, 64]
    }).groupby(
        ['year', 'quarter', 'month', 'day', 'continent'], sort=False).sum()

    assert_frame_equal(df, test_df)
