from __future__ import absolute_import, unicode_literals

import os
from collections import OrderedDict

import pytest

try:
    import sqlalchemy
except:
    pass

from six.moves.urllib.parse import urlparse

from olapy.core.mdx.executor import MdxEngine

from .db_creation_utils import create_insert, drop_tables

cube_config = {
    'xmla_authentication': False,
    'source': 'db',
    'name': 'main',
    'facts': {
        'keys': {
            'warehouse_id': 'warehouse.id',
            'store_id': 'store.id',
            'product_id': 'product.id'
        },
        'measures': ['units_ordered', 'units_shipped', 'supply_time'],
        'table_name': 'food_facts'
    },
    'dimensions': [{
        'displayName': 'food_facts',
        'name': 'food_facts',
        'columns': {}
    }, {
        'displayName': 'product',
        'name': 'product',
        'columns':
            OrderedDict([('id', 'id'), ('brand_name', 'brand_name'),
                         ('product_name', 'product_name'), ('sku',
                                                            'stock_keeping_unit')])
    }, {'displayName': 'store',
        'name': 'store',
        'columns':
            OrderedDict([('id', 'id'), ('store_type', 'store_type'),
                         ('store_name', 'store_name'), ('store_city', 'store_city'),
                         ('store_country', 'country')])
        }, {
        'displayName': 'warehouse',
        'name': 'warehouse',
        'columns':
            OrderedDict([('id', 'id'), ('warehouse_name', 'warehouse_name'),
                         ('warehouse_city', 'warehouse_city'),
                         ('warehouse_country', 'warehouse_country')])
    }]
}


@pytest.fixture(scope='module')
def executor(request):
    config = None
    custom = False
    if hasattr(request, 'param'):
        custom = request.param
        config = cube_config
        sqlalchemy_uri = 'sqlite://'
    else:
        sqlalchemy_uri = os.environ.get('SQLALCHEMY_DATABASE_URI', '')
        if not sqlalchemy_uri:
            sqlalchemy_uri = 'sqlite://'
    db_test = urlparse(sqlalchemy_uri).path.replace('/', '')
    engine = sqlalchemy.create_engine(sqlalchemy_uri)
    create_insert(engine, custom)
    mdx_engine = MdxEngine(sqla_engine=engine, source_type='db', cube_config=config)
    mdx_engine.load_cube(
        cube_name=db_test if db_test else 'main', fact_table_name='facts')
    yield mdx_engine

    drop_tables(engine, custom)
    # os.environ.pop('SQLALCHEMY_DATABASE_URI', None)
