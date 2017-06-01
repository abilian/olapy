from __future__ import absolute_import, division, print_function

from sqlalchemy import inspect

from ..tools.connection import MyDB
import pandas.io.sql as psql


def _load_tables_db(executer_instance):
    """
    Load tables from database.

    :return: tables dict with table name as key and dataframe as value
    """
    tables = {}
    db = MyDB(db=executer_instance.cube)
    inspector = inspect(db.engine)

    for table_name in inspector.get_table_names():
        value = psql.read_sql_query(
            'SELECT * FROM "{0}"'.format(table_name), db.engine)

        tables[table_name] = value[[
            col for col in value.columns if col.lower()[-3:] != '_id'
        ]]
    return tables


def _construct_star_schema_db(executer_instance):
    """
    Construct star schema DataFrame from database.

    :param cube_name:  cube name (database name)
    :return: star schema DataFrame
    """
    db = MyDB(db=executer_instance.cube)

    # load facts table
    fusion = psql.read_sql_query(
        'SELECT * FROM "{0}" '.format(executer_instance.facts), db.engine)

    inspector = inspect(db.engine)

    for db_table_name in inspector.get_table_names():
        try:
            fusion = fusion.merge(
                psql.read_sql_query("SELECT * FROM {0}".format(
                    db_table_name[0]), db.engine))
        except:
            print('No common column')
            pass

    return fusion
