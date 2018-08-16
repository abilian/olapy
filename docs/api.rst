.. _api:

API Documentation
=================


Package ``olapy.core.services.xmla``
------------------------------------

to import the package use::

    import olapy.core.services.xmla


.. automodule:: olapy.core.services.xmla

    .. autoclass:: XmlaProviderService

.. click:: olapy.core.services.xmla:runserver
        :prog: olapy
        :show-nested:


Package ``olapy.core.mdx.executor``
-----------------------------------

to import the package use::

    import olapy.core.mdx.executor.execute


.. automodule:: olapy.core.mdx.executor.execute

    .. autoclass:: MdxEngine
        :members:
                get_cubes_names,
                load_tables,
                get_all_tables_names,
                get_star_schema_dataframe,
                get_measures,
                execute_mdx

.. automodule:: olapy.core.mdx.executor.lite_execute

    .. autoclass:: MdxEngineLite
        :members:
                load_cube,
                get_measures


Package ``olapy.etl.etl``
------------------------------------

to import the package use::

    import olapy.etl.etl


.. automodule:: olapy.etl.etl
        :members: run_etl
