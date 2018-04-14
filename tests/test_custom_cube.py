import pytest

from .queries import custom_query1


@pytest.mark.parametrize('executor', [True], indirect=True)
def test_execution_query1(executor):
    result = executor.execute_mdx(custom_query1)
    assert result['result']['supply_time'][0] == 24
