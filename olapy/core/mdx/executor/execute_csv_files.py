"""
Part of :mod:`execute.py` module, here olapy constructs a cube from CSV FILES automatically,
based on a `start schema model <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os

import pandas as pd


def load_tables_csv_files(executor):
    """
    Load tables from csv files.

    :param executor: MdxEngine instance
    :return: tables dict with table name as key and dataframe as value
    """

    tables = {}
    cube = executor.get_cube_path()
    for file in os.listdir(cube):
        # to remove file extension ".csv"
        table_name = os.path.splitext(file)[0]
        value = pd.read_csv(
            os.path.join(cube, file),
            sep=executor.sep,
        )
        tables[table_name] = value[[
            col for col in value.columns if col.lower()[-3:] != '_id'
        ]]

    return tables


def construct_star_schema_csv_files(executor):
    """
    Construct star schema DataFrame from csv files.

    :param executor: MdxEngine instance
    :return: star schema DataFrame
    """
    cube = executor.get_cube_path()

    # loading facts table
    fusion = pd.read_csv(
        os.path.join(cube, executor.facts + '.csv'),
        sep=executor.sep,
    )
    for file_name in os.listdir(cube):
        try:
            fusion = fusion.merge(
                pd.read_csv(
                    os.path.join(cube, file_name),
                    sep=executor.sep,
                ),)
        except BaseException:
            print('No common column')
            pass

    return fusion
