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
            load_tables,
            get_measures,
            get_star_schema_dataframe,
            get_all_tables_names,
            get_cube,
            get_tuples,
            decorticate_query,
            change_measures,
            get_tables_and_columns,
            execute_one_tuple,
            add_missed_column,
            update_columns_to_keep,
            execute_mdx

Package ``olapy.core.services.xmla``
------------------------------------

to import the package use::

    import olapy.core.services.xmla


.. automodule:: olapy.core.services.xmla

    .. autoclass:: XmlaProviderService
        :members:
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
            split_dataframe,
            get_tuple_without_nan,
            generate_xs0,
            generate_cell_info,
            generate_cell_data,
            generate_axes_info,
            generate_slicer_axis
