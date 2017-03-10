.. olapy documentation master file, created by
   sphinx-quickstart on Mon Jan 23 16:47:07 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to olapy's documentation!
=================================
**olapy** is an OLAP engine with MDX support, web interface (slice & dice) and XMLA support for excel client.
it can be found at
`olapy <https://github.com/abilian/olapy-core>`_.


Simple use
----------

For the impatient: here's a quick overview of how to use this project. Normally,
all you have to do is install the application first, like this::

    python setup.py install


and then, it is already done, from the root directory *(which is olapy-core)* you can

**Starting XMLA Server** by using::

    python olapy ( -c | --console , if you want to print logs only in the console)


and use the url :http://127.0.0.1:8000/xmla in excel the see your :ref:`cubes`.

- Be sure to use `Provider=MSOLAP.6 <https://blogs.technet.microsoft.com/excel_services__powerpivot_for_sharepoint_support_blog/2015/10/30/configuring-the-msolap-data-provider-version/>`_ in excel

- If you want to select many measures in excel, select them before the dimensions and then drag and drop 'Values' attribute from COLUMNS to ROWS excel field

**Starting Web DEMO** by using::


   1) python manage.py initdb : to initialize db *(one time only)*

   2) python manage.py runserver : to run application


**login** : admin

**password** : admin


* to drop the database use::

    python manage.py dropdb

Tests
^^^^^

to run tests (make sure that the server is started) ::

    python -m pytest tests


For more information about how to use this library, see the :ref:`api`.

Features
--------

- executing `MDX <https://msdn.microsoft.com/en-us/library/ms145514.aspx>`_ query
- An excel demo
- A web demo

Contents:

.. toctree::
   :maxdepth: 2

   cubes
   api
   web



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

