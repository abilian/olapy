from __future__ import absolute_import, unicode_literals

import os

import pytest
import sqlalchemy
from tests.db_creation_utils import create_insert, drop_tables

from olapy.core.mdx.executor.execute import MdxEngine


@pytest.fixture(scope='module')
def executor():
    MdxEngine.source_type = ('csv', 'db')
    db_test = 'main'  # todo change sqlite db name
    if 'SQLALCHEMY_DATABASE_URI' in os.environ:
        if os.environ['SQLALCHEMY_DATABASE_URI'] != 'sqlite://':
            db_test = 'sales_test'  # todo change sqlite db name
    else:
        os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'

    MdxEngine.source_type = ('csv', 'db')

    engine = sqlalchemy.create_engine(os.environ['SQLALCHEMY_DATABASE_URI'])
    create_insert(engine)
    mdx_engine = MdxEngine(sql_engine=engine, source_type='db')
    mdx_engine.load_cube(cube_name=db_test, fact_table_name='facts')
    yield mdx_engine
    drop_tables(engine)
    del os.environ['SQLALCHEMY_DATABASE_URI']
