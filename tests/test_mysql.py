from __future__ import absolute_import, division, print_function

import os
import pandas as pd
import pytest
import sqlalchemy
from pandas.util.testing import assert_frame_equal

from olapy.core.mdx.executor.execute import get_default_cube_directory
from tests.mysql_utils import create_insert, drop_tables
from .queries import query1, query3, query6, query7, query8, query9, \
    query10

CUBE = 'sales_mysql'
USER_NAME = 'root_db'
PASSWORD = 'toor'
DB = 'sales_mysql'


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
def test_conf_file_change():
    if 'MYSQL_URI' not in os.environ.keys():
        # py.test directly #todo fix remove this
        with open(os.path.join(get_default_cube_directory(), 'olapy-config'), "w") as f:
            f.write("""
            dbms : mysql
            host : localhost
            port : 3306
            user : root_db
            password : toor
            driver : mysql
            """)


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
@pytest.fixture(scope='function')
def connect(user=USER_NAME,
            password=PASSWORD,
            db=DB,
            host='localhost',
            port=3306):
    """Returns a connection and a metadata object"""
    if 'MYSQL_URI' in os.environ.keys():
        return sqlalchemy.create_engine(os.environ['MYSQL_URI'])
    else:
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        url = 'mysql://{}:{}@{}:{}/{}'
        url = url.format(user, password, host, port, db)

        # The return value of create_engine() is our connection object
        return sqlalchemy.create_engine(url, connect_args={'charset': 'utf8'})


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
# create tables in the postgres database
def test_create_tables(connect):
    create_insert(connect)


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
@pytest.fixture(scope='module')
def executor():
    from olapy.core.mdx.executor.execute import MdxEngine
    MdxEngine.source_type = ('csv', 'db')
    return MdxEngine(CUBE)


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
def test_execution_query1(executor):
    assert executor.execute_mdx(query1)['result']['Amount'][0] == 1023


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
def test_execution_query2(executor):
    df = executor.execute_mdx(query3)['result']
    test_df = pd.DataFrame({
        'Country': ['France', 'Spain', 'Switzerland', 'United States'],
        'Amount': [4, 3, 248, 768],
    }).groupby(['Country']).sum()

    assert assert_frame_equal(df, test_df) is None


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
def test_execution_query6(executor):
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


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
def test_execution_query7(executor):
    df = executor.execute_mdx(query7)['result']
    test_df = pd.DataFrame({
        'Company': [
            'Crazy Development', 'Crazy Development', 'Crazy Development',
            'Crazy Development', 'Crazy Development', 'Crazy Development',
            'Crazy Development', 'Crazy Development'
        ],
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
            'May 18,2010',
            'May 16,2010',
            'May 14,2010',
            'May 12,2010',
            'May 13,2010',
            'May 15,2010',
            'May 17,2010',
            'May 19,2010',
        ],
        'Continent': [
            'Europe', 'Europe', 'Europe', 'Europe', 'Europe', 'Europe',
            'Europe', 'Europe'
        ],
        'Amount': [64, 16, 4, 1, 2, 8, 32, 128]
    }).groupby(
        ['Company', 'Year', 'Quarter', 'Month', 'Day', 'Continent'],
        sort=False).sum()

    assert assert_frame_equal(df, test_df) is None


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
def test_execution_query8(executor):
    df = executor.execute_mdx(query8)['result']
    test_df = pd.DataFrame({
        'Continent': ['Europe', 'Europe', 'Europe'],
        'Country': ['Spain', 'France', 'Switzerland'],
        'Amount': [3, 4, 248]
    }).groupby(
        ['Continent', 'Country'], sort=False).sum()

    assert assert_frame_equal(df, test_df) is None


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
def test_execution_query9(executor):
    df = executor.execute_mdx(query9)['result']
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
        'Amount': [128, 32, 8, 2, 1, 4, 16, 64]
    }).groupby(
        ['Year', 'Quarter', 'Month', 'Day', 'Continent'], sort=False).sum()

    assert assert_frame_equal(df, test_df) is None


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
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


@pytest.mark.skipif("os.environ['DB_TEST'] == 'POSTGRES'")
# drop created tables from postgres database
def test_drop_tables(connect):
    drop_tables(connect)
