from __future__ import absolute_import, division, print_function

from ..tools.connection import MyDB
import pandas.io.sql as psql


def _load_tables_db(executer_instance):
    """
    Load tables from database.

    :return: tables dict with table name as key and dataframe as value
    """
    tables = {}
    db = MyDB(db=executer_instance.cube)
    cursor = db.connection.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables
                      WHERE table_schema = 'public'""")

    for table_name in cursor.fetchall():
        value = psql.read_sql_query(
            'SELECT * FROM "{0}" '.format(table_name[0]), db.connection)

        tables[table_name[0]] = value[[
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
        'SELECT * FROM "{0}" '.format(executer_instance.facts), db.connection)

    cursor = db.connection.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables
                          WHERE table_schema = 'public'""")
    for db_table_name in cursor.fetchall():
        try:
            fusion = fusion.merge(
                psql.read_sql_query("SELECT * FROM {0}".format(
                    db_table_name[0]), db.connection))
        except:
            print('No common column')
            pass

    return fusion
