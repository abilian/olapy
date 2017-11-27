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


Testing
+++++++

OlaPy is configured to run units and integrations tests suites. Before running tests, make sure you have Postgres and Mysql installed, and then set the environment variables to connect to each database management system with::

      # for mysql
      export MYSQL_URI = mysql://{USER}:{PASSWORD}@localhost:3306/{YourTestDataBase}
      export SQLALCHEMY_DATABASE_URI_MYSQL=mysql://{USER}:{PASSWORD}@localhost:3306 #don't put database name here

      # for postgres
      export POSTGRES_URI=postgresql://{USER}:{PASSWORD}@localhost:5432/{YourTestDataBase}
      export SQLALCHEMY_DATABASE_URI_POSTGRES=postgresql://{USER}:{PASSWORD}@localhost:5432 #don't put database name here

and then run::

    tox

Olapy-data
----------

After installation, you can take a look to `olapy-data` folder located under::

    ~/olapy-data                       for Linux/Mac user

    C:\User\{USER_NAME}\olapy-data     for Windows

This folder contains some required files to configure olapy and some demo cubes under /cubes folder, we will deeply discuss about this in the :advanced configuration section:
