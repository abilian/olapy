"""
Part of :mod:`execute.py` module, here olapy construct cube from CSV FILES automatically based on \
`start schema model <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os

import pandas as pd


def load_tables_csv_files(executor_instance):
    """
    Load tables from csv files.

    :return: tables dict with table name as key and dataframe as value
    """
    tables = {}
    cube = executor_instance.get_cube_path()
    for file in os.listdir(cube):
        # to remove file extension ".csv"
        table_name = os.path.splitext(file)[0]
        value = pd.read_csv(
            os.path.join(cube, file),
            sep=executor_instance.sep)
        tables[table_name] = value[[
            col for col in value.columns if col.lower()[-3:] != '_id'
        ]]

    return tables


def construct_star_schema_csv_files(executor_instance):
    """
    Construct star schema DataFrame from csv files.

    :param cube_name:  cube name (folder name)
    :return: star schema DataFrame
    """
    cube = executor_instance.get_cube_path()

    # loading facts table
    fusion = pd.read_csv(
        os.path.join(cube, executor_instance.facts + '.csv'),
        sep=executor_instance.sep, )
    for file_name in os.listdir(cube):
        try:
            fusion = fusion.merge(
                pd.read_csv(
                    os.path.join(cube, file_name),
                    sep=executor_instance.sep, ), )
        except BaseException:
            print('No common column')
            pass

    return fusion
