"""
Part of :mod:`execute.py` module, here olapy construct cube from DATABASE automatically \
based on `start schema model <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_
"""

from __future__ import absolute_import, division, print_function, unicode_literals

import os
import pandas as pd
import pandas.io.sql as psql
from collections import Iterable
from sqlalchemy import inspect

from ..tools.connection import MyDB, MySqliteDB, MyOracleDB, MyMssqlDB


def _get_instantiate_db(executer_instace):
    if 'SQLALCHEMY_DATABASE_URI' in os.environ:
        dbms = MyDB.get_dbms_from_conn_string(os.environ['SQLALCHEMY_DATABASE_URI']).upper()
    else:
        dbms = executer_instace.db_config.get_db_credentials().get('dbms').upper()
    if dbms == 'SQLITE':
        db = MySqliteDB(executer_instace.db_config)
    elif dbms == 'ORACLE':
        db = MyOracleDB(executer_instace.db_config)
    elif dbms == 'MSSQL':
        db = MyMssqlDB(executer_instace.db_config)
    else:
        db = MyDB(executer_instace.db_config, db=executer_instace.cube)
    return db


def load_tables_db(executor_instance):
    """
    Load tables from database.

    :return: tables dict with table name as key and dataframe as value
    """
    tables = {}
    # todo db from executro instance
    db = _get_instantiate_db(executor_instance)
    inspector = inspect(db.engine)

    # fix all postgres table  names are lowercase
    # load_tables is executed before construct_star_schema
    if db.dbms.upper() == 'POSTGRES':
        executor_instance.facts = executor_instance.facts.lower()

    for table_name in inspector.get_table_names():
        if db.dbms.upper() == 'ORACLE' and table_name.upper() == 'FACTS':
            # fix for oracle
            table_name = table_name.title()

        results = db.engine.execution_options(stream_results=True).execute('SELECT * FROM {0}'.format(table_name))
        # Fetch all the results of the query
        value = pd.DataFrame(iter(results), columns=results.keys())  # Pass results as an iterator
        # with string_folding_wrapper we loose response time
        # value = pd.DataFrame(string_folding_wrapper(results),columns=results.keys())
        tables[table_name] = value[[col for col in value.columns if col.lower()[-3:] != '_id']]

    return tables


def construct_star_schema_db(executor_instance):
    """
    Construct star schema DataFrame from database.

    :param cube_name:  cube name (database name)
    :return: star schema DataFrame
    """

    db = _get_instantiate_db(executor_instance)

    fusion = psql.read_sql_query('SELECT * FROM {0}'.format(executor_instance.facts), db.engine)
    inspector = inspect(db.engine)

    for db_table_name in inspector.get_table_names():
        try:
            db_table_name = str(db_table_name)
        except Exception:
            if isinstance(db_table_name, Iterable):
                db_table_name = db_table_name[0]
        try:
            fusion = fusion.merge(psql.read_sql_query("SELECT * FROM {0}".format(db_table_name), db.engine))
        except BaseException:
            print('No common column between {0} and {1}'.format(executor_instance.facts, db_table_name))
            pass

    return fusion
