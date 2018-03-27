from __future__ import absolute_import, unicode_literals

import os
from six.moves.urllib.parse import urlparse

import pytest
import sqlalchemy
from tests.db_creation_utils import create_insert, drop_tables

from olapy.core.mdx.executor.execute import MdxEngine


@pytest.fixture(scope='module')
def executor():
    MdxEngine.source_type = ('csv', 'db')
    sqlalchemy_uri = os.environ.get('SQLALCHEMY_DATABASE_URI', 'sqlite://')
    db_test = urlparse(sqlalchemy_uri).path.replace('/', '')
    engine = sqlalchemy.create_engine(sqlalchemy_uri)
    create_insert(engine)
    mdx_engine = MdxEngine(sqla_engine=engine, source_type='db')
    # if db_test:
    #     mdx_engine.load_cube(cube_name=db_test, fact_table_name='facts') # sqlit:// main db will be loaded inside 'change_catalogue'
    mdx_engine.load_cube(cube_name=db_test if db_test else 'main', fact_table_name='facts')
    yield mdx_engine
    drop_tables(engine)
    os.environ.pop('SQLALCHEMY_DATABASE_URI', None)
