.. olapy documentation master file, created by
   sphinx-quickstart on Mon Jan 23 16:47:07 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

OlaPy - OLAP Engine
===================
**OlaPy** is an OLAP engine based on python, which gives you a set of tools for development of reporting and analytical applications, multidimensional analysis and browsing of aggregated data with `MDX <https://en.wikipedia.org/wiki/MultiDimensional_eXpressions>`__ and `XMLA <https://en.wikipedia.org/wiki/XML_for_Analysis>`_. support.
it can be found in
`Github <https://github.com/abilian/olapy>`_. or `pypi <https://pypi.python.org/pypi/olapy>`_.

- It is fast and uses in-memory technology and various techniques (aggregation and real-time computation) to provide sub-second responses.

- It includes an ETL layer (Extract Transform Load) for better data handling.

- It support Most common databases (Postgres, MySql, Oracle, SQL Server) and CSV file format (only csv right now) to construct cubes.

Quick start
-----------

For the impatient: here's a quick overview of how to use this project.

All you have to do is install the application first, like this::

    python setup.py install


and then, it is already done, you can

**Start XMLA Server** by using::

    python olapy ( -c | --console , if you want to print logs only in the console)


and use the url :http://127.0.0.1:8000/ in excel the see your :ref:`cubes <cubes>`.

- Be sure to use `Provider=MSOLAP.6 <https://blogs.technet.microsoft.com/excel_services__powerpivot_for_sharepoint_support_blog/2015/10/30/configuring-the-msolap-data-provider-version/>`_ in excel

you can also :ref:`customize <customize>` your cubes

To run tests, run::

    pytest tests

or simply (on Unix-like systems)::

    make test


For more information about how to use this library, see the :ref:`api`.

Features
--------

- executing `MDX <https://msdn.microsoft.com/en-us/library/ms145514.aspx>`__ query
- An excel demo

Contents:

.. toctree::
   :maxdepth: 2

   installation
   quick_start
   cubes
   customize
   working_with_db
   olapy_etl
   adv_olapy_config
   api



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

