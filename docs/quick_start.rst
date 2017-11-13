Quick Start
-----------

olapy as xmla server
********************

After :ref:`installation <installation>`, you can run olapy server with::

    olapy runserver

all you have to do now is to try it with excel spreadsheet,

open excel, open new spreadsheet and go to : Data -> From Other Sources -> From Analysis Services

.. image:: pictures/excel

after that, excel will ask you the server name, put http://127.0.0.1:8000/ and click next, then you can chose one of default olapy demo cubes (sales, foodmart...) and finish.

that's it ! now you can play with data

olapy as library
****************

if you want to use olapy as library to execute mdx queries, very simple, start by importing the mdx engine::

    from olapy.core.mdx.executor.execute import MdxEngine

in our example, we're going to use sales demo cube, so we instantiate MdxEngine with this cube::

    executor = MdxEngine('sales')

we set an mdx query::

    query = """
    SELECT
    Hierarchize({[Measures].[Amount]}) ON COLUMNS
    FROM [sales]
    """

    executor.mdx_query = query

and execute::

    data_frame = executor.execute_mdx()['result']
    print(data_frame)

result:
    +---+--------+
    |   | Amount |
    +---+--------+
    | 0 | 1023   |
    +---+--------+