from __future__ import absolute_import, division, print_function

from olapy.core.mdx.executor.execute import MdxEngine

CUBE = 'sales'

query1 = """SELECT
         {[Measures].[Amount]} ON COLUMNS
         FROM [sales]"""


where1 = "Where [Time].[Calendar].[Day].[May 12,2010]"



executer = MdxEngine(CUBE)


def test_parsing_query1():

    query_parts = executer.decorticate_query(query1)

    assert query_parts['all'] == [['Measures', 'Amount']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Measures', 'Amount']]

    query1_where = query1 + '\n' + where1
    query_parts = executer.decorticate_query(query1_where)

    assert query_parts['all'] == [['Measures', 'Amount'], ['Time', 'Calendar', 'Day', 'May 12,2010']]
    assert query_parts['rows'] == []
    assert query_parts['where'] == []
    assert query_parts['columns'] == [['Measures', 'Amount']]
    
