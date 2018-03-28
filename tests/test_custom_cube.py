import pytest

from tests.queries import custom_query1


@pytest.mark.parametrize(
    'executor', [True], indirect=True)
def test_execution_query1(executor):
    assert executor.execute_mdx(custom_query1)['result']['supply_time'][0] == 24
