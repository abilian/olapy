.. _With connection string:
.. _With config file:

OlaPy ETL
*********

NOTE: this part is working only with Python 3.5+.

OlaPy ETL is a function that you can be called and used in order to let OlaPy make necessary transformations relative to its rules on your data.

To use OlaPy ETL, start by importing the function::

    from olapy.etl.etl import run_olapy_etl

the function require three arguments: dims_infos, facts_table and facts_ids,

- ``dims_infos`` is a dict that contains your dimensions and id column of each one::


    dims_infos = {
        # 'dimension': ['col_id'],
        'Geography': ['geography_key'],
        'Product': ['product_key']
    }

- ``facts_table`` is facts table name::

    facts_table = 'sales_facts'


- ``facts_ids`` is a list of ids columns in facts table::

    facts_ids = ['geography_key', 'product_key']

Another important parameter is ``source_type``. With this parameter you tell Olapy from where the data extraction will be.

- `source_type` = csv files or text files or database

Source type text or csv files:
------------------------------

If you want to use Olapy ETL with CSV or text files, you need to set source type to CSV or file and set the input folder that contains your CSV|text files::

    # source_type = 'csv' | 'file' | 'db'
    run_olapy_etl(source_folder='/home/{USER_NAME}/input_dir',source_type='file', dims_infos=dims_infos, facts_table='sales_facts', facts_ids=facts_ids)

Source type Database:
---------------------

To use ``olapy_etl`` with data from a database, first set ``source_type=db`` and then set the database connection credentials as follow:

With connection string
^^^^^^^^^^^^^^^^^^^^^^

To use an environment variable that hold the connection string, just before ``run_olapy_etl`` function, use the following::

    export SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306/{Database_name} for mac/linux

    set SQLALCHEMY_DATABASE_URI = mysql://root:root@localhost:3306/{Database_name} for windows

**NOTE** unlike :doc:`Olapy Database config with Environnement variable <working_with_db>`, Here you **must** Specify the Database name.

With config file
^^^^^^^^^^^^^^^^

If you want to use :doc:`Olapy config file <working_with_db>`, you have to add two variables to that file:

    - ``db_name`` which is the database from where you want to get tables.
    - ``driver`` which is the used driver.

Here an example of full olapy config file::

    dbms : postgres
    host : localhost
    port : 5432
    user : postgres
    password : root

    ######### for ETL #####
    db_name : my_db
    driver : postgresql+psycopg2     # mysql+mysqldb | oracle+cx_oracle | mssql+pyodbc...

You can take a look to `SqlAlchemy specs <http://docs.sqlalchemy.org/en/latest/core/engines.html>`_ to get more details.


Now, after preparing the database connection with the :ref:`connection string <With connection string>`, or :ref:`the config file <With config file>`, let's run the function with those parameters::

    run_olapy_etl(source_type='db', dims_infos=dims_infos, facts_table='sales_facts', facts_ids=facts_ids)

Some additionnal parameters can be added to ``run_olapy_etl``:

- ``in_delimiter`` to specify csv input files separator - by default: ','.
