.. _api:

API Documentation
=================


Package ``olapy.core.mdx.parser``
---------------------------------


First, to import the package use::

    import olapy.core.mdx.parser.parse


.. automodule:: olapy.core.mdx.parser.parse

    .. autoclass:: MdxParser
        :members:
            parsing_mdx_query

Package ``olapy.core.mdx.executor``
-----------------------------------

to import the package use::

    import olapy.core.mdx.executor.execute


.. automodule:: olapy.core.mdx.executor.execute

    .. autoclass:: MdxEngine
        :members:
            get_cubes_names,
            get_cube,
            get_all_tables_names,
            get_cube,
            get_measures,
            load_tables,
            change_measures,
            get_tables_and_columns,
            execute_one_tuple,
            add_missed_column,
            execute_mdx

        **MdxEngine.load_star_schema_dataframe** contains a DataFrame of all dimension merged in one (the star schema in one table)

Package ``olapy.core.services.xmla``
------------------------------------

to import the package use::

    import olapy.core.services.xmla


.. automodule:: olapy.core.services.xmla

    .. autoclass:: XmlaProviderService
        :members:
            change_catalogue,
            Discover,
            Execute

        .. autofunction:: start_server


.. automodule:: olapy.core.services.xmla_discover_tools

    .. autoclass:: XmlaDiscoverTools
        :members:
            change_catalogue

.. automodule:: olapy.core.services.xmla_execute_tools

    .. autoclass:: XmlaExecuteTools
        :members:
            split_DataFrame,
            get_tuple_without_nan,
            check_measures_only_selected,
            generate_xs0_measures_only,
            generate_xs0,
            generate_cell_data,
            generate_axes_info,
            generate_slicer_axis


.. automodule:: olapy.core.services.logger

    .. autoclass:: Logs
        :members:
            write_log