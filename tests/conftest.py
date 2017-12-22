import os
import pytest
import sqlalchemy

from olapy.core.mdx.executor.execute import MdxEngine
from tests.db_creation_utils import create_insert, drop_tables


# todo fixture with params
@pytest.fixture(scope='module')
def executor(request):
    MdxEngine.source_type = ('csv', 'db')
    if 'param' in request.__dict__.keys():
        os.environ['SQLALCHEMY_DATABASE_URI'] = os.environ[request.param[0]]
        if request.param[0] == 'SQLITE_URI':
            MdxEngine.engine = None
            yield MdxEngine('sales_sqlite', fact_table_name='facts')
        else:
            MdxEngine.engine = sqlalchemy.create_engine(os.environ[request.param[0]])
            create_insert(MdxEngine.engine)
            yield MdxEngine(request.param[1], fact_table_name='facts')
            drop_tables(MdxEngine.engine)
    else:
        # sqlite mem bd
        os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
        MdxEngine.source_type = ('csv', 'db')
        MdxEngine.engine = sqlalchemy.create_engine("sqlite://")
        create_insert(MdxEngine.engine)
        yield MdxEngine('main', fact_table_name='facts')
