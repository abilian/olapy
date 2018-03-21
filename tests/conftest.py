from __future__ import absolute_import, unicode_literals

import os

import pytest
import sqlalchemy
from tests.db_creation_utils import create_insert, drop_tables

from olapy.core.mdx.executor.execute import MdxEngine


@pytest.fixture(scope='module')
def executor(request):
    MdxEngine.source_type = ('csv', 'db')

    if hasattr(request, 'param'):
        if 'sqlite:///tests' in os.environ['SQLALCHEMY_DATABASE_URI']:
            mdx_engine = MdxEngine(source_type='db')
            # sales exists with csv
            mdx_engine.load_cube('sales_sqlite', fact_table_name='facts')
            yield mdx_engine
        else:
            engine = sqlalchemy.create_engine(os.environ['SQLALCHEMY_DATABASE_URI'])
            create_insert(engine)
            mdx_engine = MdxEngine(sql_engine=engine, source_type='db')
            mdx_engine.load_cube(cube_name=request.param[1], fact_table_name='facts')
            yield mdx_engine
            drop_tables(engine)
    else:
        # sqlite mem bd
        os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
        MdxEngine.source_type = ('csv', 'db')
        engine = sqlalchemy.create_engine("sqlite://")
        create_insert(engine)

        mdx_engine = MdxEngine(sql_engine=engine, source_type='db')
        mdx_engine.load_cube('main', fact_table_name='facts')
        yield mdx_engine
