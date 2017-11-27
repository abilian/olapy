Quick Start
-----------

olapy as XMLA server
********************

After :ref:`installation <installation>`, you can run the Olapy server with::

    olapy runserver

All you have to do now is to try it with an Excel spreadsheet:

    open excel, open new spreadsheet and go to : Data -> From Other Sources -> From Analysis Services

    .. image:: pictures/excel.png

After that, Excel will ask you the server name: put ``http://127.0.0.1:8000/`` and click next, then you can chose one of default olapy demo cubes (sales, foodmart...) and finish.

That's it! Now you can play with your data.

Olapy as a library
******************

If you want to use olapy as a library to execute MDX queries, start by importing the MDX engine::

    from olapy.core.mdx.executor.execute import MdxEngine

In our example, we're going to use sales demo cube, so we instantiate *MdxEngine* with this cube::

    executor = MdxEngine('sales')

We set an MDX query::

    query = """
    SELECT
    Hierarchize({[Measures].[Amount]}) ON COLUMNS
    FROM [sales]
    """

and execute::

    data_frame = executor.execute_mdx(query)['result']
    print(data_frame)

Result:

    +---+--------+
    |   | Amount |
    +---+--------+
    | 0 | 1023   |
    +---+--------+
