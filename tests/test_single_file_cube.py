from __future__ import absolute_import, division, print_function, \
    unicode_literals

import pytest

from olapy.core.mdx.executor.lite_execute import MdxEngineLite

from .queries import query1


@pytest.fixture(scope="module")
def lite_executor():
    data_file = "tests/data_test.csv"
    lite_executor = MdxEngineLite(
        direct_table_or_file=data_file,
        source_type=None,
        db_config=None,
        cubes_config=None,
        columns="city,licence,amount,count",
        measures=None,
        sqla_engine=None,
    )
    lite_executor.load_cube(table_or_file=data_file)
    return lite_executor


def test_execution_query1(lite_executor):
    assert lite_executor.execute_mdx(query1)["result"]["amount"][0] == 1023
