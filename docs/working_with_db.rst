.. _Environnement variable:
.. _Olapy config file:

Running olapy with Database
***************************

As we said in the previous section, olapy start with csv files by default when using olapy runserver command, so how can we work with databases ?, well, you need to provide some database informations (login, password, etc...) to olapy so it can connect to your data base management system.
the command to run olapy with databases is ::

    olapy runserver -st=csv,db

here olapy get cubes from csv and database , of course if you want only database use -st=db ...
You have three possibilities to configure olapy with database

Environnement variable
----------------------

at startup, olapy looks for an environment variable called *SQLALCHEMY_DATABASE_URI* which is the connection string string that hold your database credentials and it's something like::

    SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306

to use this method, just before starting olapy with olapy runserver, use the following command::

    export SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306 for mac/linux

    set SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306 for windows

and then start olapy with option -st=csv,db of course

**NOTE** don't put the database name in the connection string, you will select the database after from excel

SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306/my_database -> this will not work


Database string connection
--------------------------

this is simple as running olapy with -sa option::


    olapy runserver -st=csv,db -sa=mysql://root:root@localhost:3306


and the same rule **don't put the database name in the connection string, you will select the database after from excel**

Olapy config file
-----------------

The third way to configure Database connection, is using a file configuration named olapy-config under olapy-data folder, a default/demo olapy-config file is created after installing olapy under olapy-data.
you can modify this file relative to your configuration::


    dbms : postgres             # mysql | oracle | mssql | oracle
    host : localhost            # server ip
    port : 5432                 # 5432 default for postgres || 3306 default for mysql || 1433  default for mssql || 1521 default for ORACLE
    user : root                 # database user name
    password : root             # database password

if you want to use sql server as dbms, you need to add an additional information to this file::

    sql_server_driver : SQL Server Native Client 11.0 # or whatever the client you want to use 10.0 , 11.0 , 12.0...

and olapy runserver -st=csv,db as always