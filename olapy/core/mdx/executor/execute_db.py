from __future__ import absolute_import, division, print_function

from sqlalchemy import inspect
import pandas as pd
from ..tools.connection import MyDB
import pandas.io.sql as psql

# split execution into three part (execute from config files,
# execute csv files if they respect olapy's start schema model,
# and execute data base tables if they respect olapy's start schema model)

# class StringFolder(object):
#     """
#     Class that will fold strings. See 'fold_string'.
#     This object may be safely deleted or go out of scope when
#     strings have been folded.
#     """
#     def __init__(self):
#         self.unicode_map = {}
#
#     def fold_string(self, s):
#         """
#         Given a string (or unicode) parameter s, return a string object
#         that has the same value as s (and may be s). For all objects
#         with a given value, the same object will be returned. For unicode
#         objects that can be coerced to a string with the same value, a
#         string object will be returned.
#         If s is not a string or unicode object, it is returned unchanged.
#         :param s: a string or unicode object.
#         :return: a string or unicode object.
#         """
#         # If s is not a string or unicode object, return it unchanged
#         if not isinstance(s, basestring):
#             return s
#
#         # If s is already a string, then str() has no effect.
#         # If s is Unicode, try and encode as a string and use intern.
#         # If s is Unicode and can't be encoded as a string, this try
#         # will raise a UnicodeEncodeError.
#         try:
#             return intern(str(s))
#         except UnicodeEncodeError:
#             # Fall through and handle s as Unicode
#             pass
#
#         # Look up the unicode value in the map and return
#         # the object from the map. If there is no matching entry,
#         # store this unicode object in the map and return it.
#         return self.unicode_map.setdefault(s, s)
#
#
# def string_folding_wrapper(results):
#     """
#     This generator yields rows from the results as tuples,
#     with all string values folded.
#     """
#     # Get the list of keys so that we build tuples with all
#     # the values in key order.
#     keys = results.keys()
#     folder = StringFolder()
#     for row in results:
#         yield tuple(
#             folder.fold_string(row[key])
#             for key in keys
#         )

# TODO try pandas.read_sql_table and pandas.read_sql


def _load_tables_db(executer_instance):
    """
    Load tables from database.

    :return: tables dict with table name as key and dataframe as value
    """
    tables = {}
    db = MyDB(
        db_config_file_path=executer_instance.DATA_FOLDER,
        db=executer_instance.cube)
    inspector = inspect(db.engine)

    for table_name in inspector.get_table_names():

        # value = psql.read_sql_query(
        #     'SELECT * FROM "{0}"'.format(table_name), db.engine)

        # results = db.engine.execute('SELECT * FROM "{0}"'.format(table_name))
        results = db.engine.execution_options(stream_results=True).execute(
            'SELECT * FROM "{0}"'.format(table_name))
        # Fetch all the results of the query
        value = pd.DataFrame(
            iter(results),
            columns=results.keys())  # Pass results as an iterator
        # with string_folding_wrapper we loose response time
        # value = pd.DataFrame(string_folding_wrapper(results),columns=results.keys())
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
    with db.engine as connection:
        fusion = psql.read_sql_query(
            'SELECT * FROM "{0}" '.format(executer_instance.facts), connection)

        inspector = inspect(connection)

        for db_table_name in inspector.get_table_names():
            try:
                fusion = fusion.merge(
                    psql.read_sql_query("SELECT * FROM {0}".format(
                        db_table_name[0]), connection))
            except:
                print('No common column')
                pass

    return fusion
