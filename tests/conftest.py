from __future__ import absolute_import, unicode_literals

import os

import pytest
import sqlalchemy
from tests.db_creation_utils import create_insert, drop_tables

from olapy.core.mdx.executor.execute import MdxEngine


@pytest.fixture(scope='module')
def executor():
    MdxEngine.source_type = ('csv', 'db')
    sqlalchemy_uri = os.environ.get('SQLALCHEMY_DATABASE_URI', 'sqlite://')
    db_test = sqlalchemy_uri.split('/')[-1] if sqlalchemy_uri.split('/')[-1] else 'main'
    MdxEngine.source_type = ('csv', 'db')
    engine = sqlalchemy.create_engine(sqlalchemy_uri)
    create_insert(engine)
    mdx_engine = MdxEngine(sql_engine=engine, source_type='db')
    mdx_engine.load_cube(cube_name=db_test, fact_table_name='facts')
    yield mdx_engine
    drop_tables(engine)
    os.environ.pop('SQLALCHEMY_DATABASE_URI', None)
