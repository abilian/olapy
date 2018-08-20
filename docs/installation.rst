.. _installation:

Installation
============

Install from PyPI
-----------------

You can install it directly from the `Python Package Index <https://pypi.python.org/pypi/olapy>`_::

    pip install olapy

Install from Github
-------------------

The project sources are stored in `Github repository <https://github.com/abilian/olapy>`_.

Download from GitHub::

    git clone git://github.com/abilian/olapy.git

Then install::

    cd olapy
    python setup.py install


Initialization
--------------

Before running olapy, you have to initialize it with::

    olapy init


Testing
+++++++

OlaPy is configured to run units and integrations tests suites. Before running tests, make sure you have installed all development requirements with::

   pip install -r dev-requirements.txt

and then run::

    tox

Test other databases
~~~~~~~~~~~~~~~~~~~~

The default database used with tests is sqlite, if you want to run tests against mysql or postgres, you need to install the appropriate driver and export a connection string like this ::

    export SQLALCHEMY_DATABASE_URI = { dialect+driver://username:password@host:port/database }

take a look to  `SQLAlchemy documentation <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_ for more information.

Olapy-data
----------

After Olapy initialization, you can take a look to `olapy-data` folder located under::

    ~/olapy-data                       for Linux/Mac user

    C:\User\{USER_NAME}\olapy-data     for Windows

This folder contains some required files to configure olapy and some demo cubes under /cubes folder, we will deeply discuss about this in the :ref:`Cubes <cubes>` and :ref:`Cube Customization <customize>`
