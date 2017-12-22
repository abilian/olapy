import os
import pytest
import sqlalchemy

from olapy.core.mdx.executor.execute import MdxEngine
from tests.db_creation_utils import create_insert, drop_tables

CUBE = 'main'


@pytest.fixture(scope='module')
def executor():
    os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
    MdxEngine.source_type = ('csv', 'db')
    MdxEngine.engine = sqlalchemy.create_engine("sqlite://")
    create_insert(MdxEngine.engine)
    return MdxEngine(CUBE, fact_table_name='facts')


@pytest.mark.skipif(
    "'DB_TEST' not in os.environ or os.environ['DB_TEST'] != 'SQLITE' or 'SQLITE_URI' not in os.environ")
@pytest.fixture(scope='module')
def executor_sqlite():
    from olapy.core.mdx.executor.execute import MdxEngine
    MdxEngine.source_type = ('csv', 'db')
    os.environ['SQLALCHEMY_DATABASE_URI'] = os.environ['SQLITE_URI']
    MdxEngine.engine = None
    return MdxEngine('sales_sqlite')


# todo fixture with params
@pytest.fixture(scope='module')
def executor_mysql():
    from olapy.core.mdx.executor.execute import MdxEngine
    MdxEngine.source_type = ('csv', 'db')
    os.environ['SQLALCHEMY_DATABASE_URI'] = os.environ['MYSQL_URI']
    MdxEngine.engine = sqlalchemy.create_engine(os.environ['MYSQL_URI'])
    create_insert(MdxEngine.engine)
    yield MdxEngine('sales_mysql', fact_table_name='facts')
    drop_tables(MdxEngine.engine)


# todo fixture with params
@pytest.fixture(scope='module')
def executor_postgres():
    from olapy.core.mdx.executor.execute import MdxEngine
    MdxEngine.source_type = ('csv', 'db')
    os.environ['SQLALCHEMY_DATABASE_URI'] = os.environ['POSTGRES_URI']
    MdxEngine.engine = sqlalchemy.create_engine(os.environ['POSTGRES_URI'])
    create_insert(MdxEngine.engine)
    yield MdxEngine('sales_postgres', fact_table_name='facts')
    drop_tables(MdxEngine.engine)
