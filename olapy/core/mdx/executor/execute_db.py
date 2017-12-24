"""
Part of :mod:`execute.py` module, here olapy construct cube from DATABASE
automatically based on
`start schema model <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_
"""

from __future__ import absolute_import, division, print_function, \
    unicode_literals

from collections import Iterable

import pandas as pd
import pandas.io.sql as psql
from sqlalchemy import inspect


def load_tables_db(executor):
    """
    Load tables from database.

    :param executor: MdxEngine instance
    :return: tables dict with table name as key and dataframe as value
    """

    tables = {}

    # todo db from executro instance
    db = executor.instantiate_db(executor.cube)
    # todo remove executor_instance.sqlengine
    if not executor.sqlengine:
        executor.sqlengine = executor.instantiate_db(executor.cube,).engine
    print('Connection string = ' + str(executor.sqlengine))
    inspector = inspect(executor.sqlengine)

    # fix all postgres table  names are lowercase
    # load_tables is executed before construct_star_schema
    if db.dbms.upper() == 'POSTGRES':
        executor.facts = executor.facts.lower()
    for table_name in inspector.get_table_names():
        if db.dbms.upper() == 'ORACLE' and table_name.upper() == 'FACTS':
            table_name = table_name.title()

        results = executor.sqlengine.execution_options(
            stream_results=True,
        ).execute('SELECT * FROM {0}'.format(table_name),)
        # Fetch all the results of the query
        value = pd.DataFrame(
            iter(results),
            columns=results.keys(),
        )  # Pass results as an iterator
        # with string_folding_wrapper we loose response time
        # value = pd.DataFrame(string_folding_wrapper(results),columns=results.keys())
        tables[table_name] = value[[
            col for col in value.columns if col.lower()[-3:] != '_id'
        ]]

    return tables


def construct_star_schema_db(executor):
    """
    Construct star schema DataFrame from database.

    :param executor: MdxEngine instance
    :return: star schema DataFrame
    """

    if not executor.sqlengine:
        executor.sqlengine = executor.instantiate_db(executor.cube,)

    fusion = psql.read_sql_query(
        'SELECT * FROM {0}'.format(executor.facts,),
        executor.sqlengine,
    )
    inspector = inspect(executor.sqlengine)

    for db_table_name in inspector.get_table_names():
        try:
            db_table_name = str(db_table_name)
        except Exception:
            if isinstance(db_table_name, Iterable):
                db_table_name = db_table_name[0]
        try:
            fusion = fusion.merge(
                psql.read_sql_query(
                    "SELECT * FROM {0}".format(db_table_name),
                    executor.sqlengine,
                ),)
        except BaseException:
            print('No common column between {0} and {1}'.format(
                executor.facts,
                db_table_name,
            ))
            pass

    return fusion
