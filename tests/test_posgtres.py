from __future__ import absolute_import, division, print_function
import os
import pandas as pd
import pytest
import sqlalchemy
from pandas.util.testing import assert_frame_equal

from olapy.core.mdx.executor.execute import get_default_cube_directory
from tests.postgres_utils import create_insert, drop_tables
from tests.queries import query_posgres1, query_posgres2, query_postgres3

CUBE = 'sales_postgres'
USER_NAME = 'postgres'
PASSWORD = 'root'
DB = 'sales_postgres'


@pytest.mark.skipif("os.environ['DB_TEST'] == 'MYSQL'")
def test_conf_file_change():
    if 'POSTGRES_URI' not in os.environ.keys():
        # py.test directly #todo fix remove this
        with open(os.path.join(get_default_cube_directory(), 'olapy-config'), "w") as f:
            f.write("""
            dbms : postgres
            host : localhost
            port : 5432
            user : postgres
            password : root
            driver : postgres
            """)


@pytest.mark.skipif("os.environ['DB_TEST'] == 'MYSQL'")
@pytest.fixture(scope='function')
def connect(user=USER_NAME,
            password=PASSWORD,
            db=DB,
            host='localhost',
            port=5432):
    """Returns a connection and a metadata object"""
    if 'POSTGRES_URI' in os.environ.keys():
        return sqlalchemy.create_engine(os.environ['POSTGRES_URI'], client_encoding='utf8')
    else:
        # DEFAULT CONFIG
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(user, password, host, port, db)

        # The return value of create_engine() is our connection object
        return sqlalchemy.create_engine(url, client_encoding='utf8')


@pytest.mark.skipif("os.environ['DB_TEST'] == 'MYSQL'")
# create tables in the postgres database
def test_create_tables(connect):
    create_insert(connect)


@pytest.mark.skipif("os.environ['DB_TEST'] == 'MYSQL'")
@pytest.fixture(scope='module')
def executor():
    from olapy.core.mdx.executor.execute import MdxEngine
    MdxEngine.source_type = ('csv', 'db')
    return MdxEngine(CUBE)


@pytest.mark.skipif("os.environ['DB_TEST'] == 'MYSQL'")
def test_execution_query1(executor):
    df = executor.execute_mdx(query_posgres1)['result']
    test_df = pd.DataFrame({
        'country': ['France', 'Spain', 'Switzerland', 'United States'],
        'amount': [4, 3, 248, 768],
    }).groupby(['country']).sum()

    assert assert_frame_equal(df, test_df) is None


@pytest.mark.skipif("os.environ['DB_TEST'] == 'MYSQL'")
def test_execution_query2(executor):
    df = executor.execute_mdx(query_posgres2)['result']
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

    assert assert_frame_equal(df, test_df) is None


@pytest.mark.skipif("os.environ['DB_TEST'] == 'MYSQL'")
def test_execution_query10(executor):
    df = executor.execute_mdx(query_postgres3)['result']
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

    assert assert_frame_equal(df, test_df) is None


@pytest.mark.skipif("os.environ['DB_TEST'] == 'MYSQL'")
# drop created tables from postgres database
def test_drop_tables(connect):
    drop_tables(connect)
