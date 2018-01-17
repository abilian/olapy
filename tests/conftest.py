from __future__ import absolute_import, unicode_literals

import os

import pytest
import sqlalchemy
from tests.db_creation_utils import create_insert, drop_tables

from olapy.core.mdx.executor.execute import MdxEngine


# todo fixture with params
@pytest.fixture(scope='module')
def executor(request):
    MdxEngine.source_type = ('csv', 'db')

    if hasattr(request, 'param'):
        sqlalchemy_database_uri = os.environ[request.param[0]]
        os.environ['SQLALCHEMY_DATABASE_URI'] = sqlalchemy_database_uri
        if request.param[0] == 'SQLITE_URI':
            yield MdxEngine('sales_sqlite', fact_table_name='facts')
        else:
            engine = sqlalchemy.create_engine(sqlalchemy_database_uri)
            create_insert(engine)
            yield MdxEngine(request.param[1], fact_table_name='facts', sql_engine=engine)
            drop_tables(engine)
    else:
        # sqlite mem bd
        os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
        MdxEngine.source_type = ('csv', 'db')
        engine = sqlalchemy.create_engine("sqlite://")
        create_insert(engine)
        yield MdxEngine('main', sql_engine=engine, fact_table_name='facts')
