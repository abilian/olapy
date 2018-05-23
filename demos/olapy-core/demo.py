from olapy.core.mdx.executor.execute import MdxEngine

mdx_query = """SELECT
            Hierarchize({[Measures].[Amount]}) ON COLUMNS
            FROM
            """


def main():
    executor = MdxEngine()
    executor.load_cube('sales')
    execution_result = executor.execute_mdx(mdx_query)
    print(execution_result['result'])


if __name__ == '__main__':
    main()
