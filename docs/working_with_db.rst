.. _Environnement variable:
.. _Olapy config file:

Running olapy with Database
***************************

As we said in the previous section, Olapy uses CSV files as source type by default when using the ``olapy runserver`` command, so how can we work with databases ? Well, you need to provide some database information (login, password, etc...) to Olapy so it can connect to your database management system.

The command to run Olapy with databases is ::

    olapy runserver -st=csv,db

Here, Olapy gets cubes from csv and database (of course if you want only database use ``-st=db`` ...)

You have three possibilities to configure olapy with database:

Environnement variable
----------------------

At startup, Olapy looks for an environment variable called *SQLALCHEMY_DATABASE_URI* which is the connection string that holds your database credentials and its something like::

    SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306

To use this method, just before starting Olapy with ``olapy runserver``, use the following command::

    export SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306 for mac/linux

    set SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306 for windows

and then start Olapy with the option ``-st=csv,db`` of course.

**NOTE** don't put the database name in the connection string, you will select the database after from Excel.

``SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306/my_database`` -> this will not work


Database string connection
--------------------------

This is simple as running Olapy with the ``-sa`` option::


    olapy runserver -st=csv,db -sa=mysql://root:root@localhost:3306


and the same rule **don't put the database name**.

Olapy config file
-----------------

The third way to configure a database connection is using a file configuration named ``olapy-config.yml`` under ``olapy-data`` folder. A default/demo ``olapy-config`` file is created after installing olapy under ``olapy-data``.

You can modify this file according to your configuration::

    connection_string : postgresql+psycopg2://postgres:root@localhost:5432

take a look to  `SQLAlchemy documentation <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_ for more information.
