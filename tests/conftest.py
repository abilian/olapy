import os
import pytest
import sqlalchemy

from olapy.core.mdx.executor.execute import MdxEngine
from tests.db_creation_utils import create_insert

CUBE = 'main'


@pytest.fixture(scope='session')
def executor():
    eng = sqlalchemy.create_engine("sqlite://")
    os.environ['SQLALCHEMY_DATABASE_URI'] = 'sqlite://'
    MdxEngine.source_type = ('csv', 'db')
    MdxEngine.engine = eng
    create_insert(eng)
    return MdxEngine(CUBE, fact_table_name='facts')
