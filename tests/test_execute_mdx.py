from __future__ import absolute_import, division, print_function

import pandas as pd
from olapy.core.mdx.executor.execute import MdxEngine

from pandas.util.testing import assert_frame_equal

from tests.queries import query1, CUBE, query3, query6

executer = MdxEngine(CUBE)


def test_execution_query1():
    executer.mdx_query = query1
    assert executer.execute_mdx()['result']['Amount'][0] == 1023


def test_execution_query2():
    executer.mdx_query = query3

    df = executer.execute_mdx()['result']
    test_df = pd.DataFrame({
        'Country': ['France', 'Spain', 'Switzerland', 'United States'],
        'Amount': [4, 3, 248, 768]
    }).groupby(['Country']).sum()

    assert assert_frame_equal(df, test_df) is None


def test_execution_query6():
    executer.mdx_query = query6

    df = executer.execute_mdx()['result']
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
