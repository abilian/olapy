OlaPy, an experimental OLAP engine based on Pandas
==================================================

About
-----

**OlaPy** is an OLAP_ engine with MDX_ support, web interface (slice & dice) and XMLA_ support for Excel clients.

.. _OLAP: https://en.wikipedia.org/wiki/Online_analytical_processing
.. _MDX: https://en.wikipedia.org/wiki/MultiDimensional_eXpressions
.. _XMLA: https://en.wikipedia.org/wiki/XML_for_Analysis

Status
~~~~~~

This project is currently a research prototype, not suited for production use.

Licence
~~~~~~~

This project is currently licenced under the LGPL v3 licence.

Installation
------------

Install from PyPI
~~~~~~~~~~~~~~~~~

You can install it directly from the `Python Package Index <https://pypi.python.org/pypi/olapy>`_::

    pip install olapy

Install from Github
~~~~~~~~~~~~~~~~~~~

The project sources are stored in `Github repository <https://github.com/abilian/olapy>`_.

Download from Github::

    git clone git://github.com/abilian/olapy.git


To set up the application, run, ideally in a virtualenv::

    cd olapy
    python setup.py install

or just::

    pip install -e .


Usage
-----

Before running OlaPy, you need to initialize it with::

    olapy init

and then you can run the server with::

    olapy runserver


and then from excel, open new spreadsheet and go to : Data -> From Other Sources -> From Analysis Services and use http://127.0.0.1:8000/ as server name and click next, then you can chose one of default olapy demo cubes (sales, foodmart...) and finish.

that's it ! now you can play with data


Developing
----------

This project must adhere to the `Abilian Developer Guide <http://abilian-developer-guide.readthedocs.io/>`_.

Pull requests are welcome.

Tests
~~~~~

To run tests, run::

    pytest tests

or simply (on Unix-like systems)::

    make test


Credits
-------

This project is developed by `Abilian SAS <https://www.abilian.com>`_ and partially funded by the French Government through the `Wendelin <http://www.wendelin.io/>`_ project and the `Investissement d'avenir <http://www.gouvernement.fr/investissements-d-avenir-cgi>`_ programme.
