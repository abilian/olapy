from __future__ import absolute_import, unicode_literals

import os
from collections import OrderedDict

from six.moves.urllib.parse import urlparse

import pytest
import sqlalchemy
from tests.db_creation_utils import create_insert, drop_tables

from olapy.core.mdx.executor.execute import MdxEngine

cube_config = {u'xmla_authentication': False,
               u'source': 'db',
               u'name': 'main',
               u'facts': {u'keys': {'warehouse_id': 'Warehouse.id', 'store_id': 'Store.id', 'product_id': 'Product.id'},
                          u'measures': ['units_ordered', 'units_shipped', 'supply_time'], u'table_name': 'food_facts'},
               u'dimensions': [{u'displayName': 'food_facts', u'name': 'food_facts', u'columns': {}},
                               {u'displayName': 'Product', u'name': 'Product', u'columns': OrderedDict(
                                   [('id', 'id'), ('brand_name', 'brand_name'), ('product_name', 'product_name'),
                                    ('SKU', 'Stock_keeping_unit')])}, {u'displayName': 'Store', u'name': 'Store',
                                                                       u'columns': OrderedDict(
                                                                           [('id', 'id'), ('store_type', 'store_type'),
                                                                            ('store_name', 'store_name'),
                                                                            ('store_city', 'store_city'),
                                                                            ('store_country', 'country')])},
                               {u'displayName': 'Warehouse', u'name': 'Warehouse', u'columns': OrderedDict(
                                   [('id', 'id'), ('warehouse_name', 'warehouse_name'),
                                    ('warehouse_city', 'warehouse_city'),
                                    ('warehouse_country', 'warehouse_country')])}]}


@pytest.fixture(scope='module')
def executor(request):
    config = None
    custom = False
    if request.param:
        custom = True
        config = cube_config
    MdxEngine.source_type = ('csv', 'db')
    sqlalchemy_uri = os.environ.get('SQLALCHEMY_DATABASE_URI', 'sqlite://')
    db_test = urlparse(sqlalchemy_uri).path.replace('/', '')
    engine = sqlalchemy.create_engine(sqlalchemy_uri)
    create_insert(engine, custom)
    mdx_engine = MdxEngine(sqla_engine=engine, source_type='db', cube_config=config)
    mdx_engine.load_cube(cube_name=db_test if db_test else 'main', fact_table_name='facts')
    yield mdx_engine
    drop_tables(engine, custom)
    os.environ.pop('SQLALCHEMY_DATABASE_URI', None)
