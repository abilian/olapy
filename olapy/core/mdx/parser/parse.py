from __future__ import absolute_import, division, print_function

from grako.model import ModelBuilderSemantics
from .gen_parser.mdxparser import MdxParserGen
from .gen_parser.models import selectStatement


class MdxParser:
    """
    this class parse the mdx query and split it into well-defined parts
    """
    START = 'MDX_statement'

    def parsing_mdx_query(self, axis, query):
        '''
        Split the query into axis

        **Example**::


            SELECT

            { [Geography].[Geo].[Country].[France],
              [Geography].[Geo].[Country].[Spain] } ON COLUMNS,

            { [Product].[Prod].[Company].[Crazy Development] } ON ROWS

              FROM [Sales]

              WHERE [Time].[Calendar].[Year].[2010]


    +------------+------------------------------------------------+
    |            | [Geography].[Geo].[Country].[France]           |
    | column     |                                                |
    |            | [Geography].[Geo].[Country].[Spain]            |
    +------------+------------------------------------------------+
    | row        | [Product].[Prod].[Company].[Crazy Development] |
    +------------+------------------------------------------------+
    | cube       | [Sales]                                        |
    +------------+------------------------------------------------+
    | condition  | [Time].[Calendar].[Year].[2010]                |
    +------------+------------------------------------------------+

        :param query: MDX Query
        :param axis: column | row | cube | condition | all
        :return: Tuples in the axis, from the MDX query
        '''
        model = MdxParserGen(semantics=ModelBuilderSemantics(
            types=[selectStatement]))
        ast = model.parse(query, rule_name=MdxParser.START, ignorecase=True)
        if axis == "column":
            if ast.select_statement.axis_specification_columns is not None:
                if u'' in ast.select_statement.axis_specification_columns:
                    ast.select_statement.axis_specification_columns.remove(u'')
            return ast.select_statement.axis_specification_columns
        elif axis == "row":
            if ast.select_statement.axis_specification_rows is not None:
                if u'' in ast.select_statement.axis_specification_rows:
                    ast.select_statement.axis_specification_rows.remove(u'')
            return ast.select_statement.axis_specification_rows
        elif axis == "cube":
            if ast.select_statement.cube_specification is not None:
                if u'' in ast.select_statement.cube_specification:
                    ast.select_statement.cube_specification.remove(u'')
            return ast.select_statement.cube_specification[1] if \
                isinstance(ast.select_statement.cube_specification, list) \
                else ast.select_statement.cube_specification
        elif axis == "condition":
            if ast.select_statement.condition_specification is not None:
                if type(ast.select_statement.condition_specification) not in (
                        unicode, str):
                    if u'' in ast.select_statement.condition_specification:
                        ast.select_statement.condition_specification.remove(u'')
            return ast.select_statement.condition_specification
        elif axis == "all":

            return 'Operation = {} \n' \
                   'Columns = {} \n' \
                   'Rows = {} \n' \
                   'From = {} \n' \
                   'Where = {} \n'.format(ast.select_statement.name,
                                          ast.select_statement.from_,
                                          ast.select_statement.axis_specification_columns,
                                          ast.select_statement.axis_specification_rows,
                                          ast.select_statement.cube_specification,
                                          ast.select_statement.condition_specification,
                                          )
