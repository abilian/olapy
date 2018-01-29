"""
Part of :mod:`execute.py` module, here we can customize olapy's cube with a config file,
by default located in /home/user/olapy-data/cubes/cubes-config.xml
(you can take a look to this file as an example)
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os

import pandas as pd
import pandas.io.sql as psql

from ..tools.connection import MyDB


def load_one_table(cubes_obj, executor, table_name, sep):
    if cubes_obj.source.upper() == 'CSV':
        facts = os.path.join(
            executor.get_cube_path(),
            table_name + '.csv',
        )
        # with extension or not
        if not os.path.isfile(facts):
            facts.replace('.csv', '')
        table = pd.read_csv(facts, sep=sep)
    else:
        db = MyDB(
            executor.database_config,
            db_name=executor.cube,
        )
        # load facts table

        table = psql.read_sql_query(
            "SELECT * FROM {}".format(table_name),
            db.engine,
        )
    return table


def load_table_config_file(executor, cube_obj, sep):
    """
    Load tables from config file.

    :param executor: MdxEngine instance
    :param cube_obj: cubes parser object
    :param sep: csv file separator
    :return: tables dict with table name as key and DataFrame as value
    """

    tables = {}
    # just one facts table right now
    executor.facts = cube_obj.facts[0].table_name

    for dimension in cube_obj.dimensions:

        df = load_one_table(cube_obj, executor, dimension.name, sep)
        if dimension.columns.keys():
            df = df[dimension.columns.keys()]

        # change table display name
        if dimension.displayName:
            table_name = dimension.displayName
        else:
            table_name = dimension.name

        # rename columns if value not None
        df.rename(
            columns=({k: v for k, v in dimension.columns.items() if v}),
            inplace=True,
        )

        tables[table_name] = df[[
            col for col in df.columns if col.lower()[-2:] != 'id'
        ]]

    return tables


# excel client
def construct_star_schema_config_file(executor, cubes_obj, sep):
    """Construct star schema DataFrame from configuration file for excel client.

    :param executor:  MdxEngine instance
    :param cubes_obj: cubes object
    :param sep: csv file separator
    :return: star schema DataFrame
    """
    executor.facts = cubes_obj.facts[0].table_name

    fusion = load_one_table(
        cubes_obj,
        executor,
        executor.facts,
        sep
    )

    for fact_key, dimension_and_key in cubes_obj.facts[0].keys.items():

        if cubes_obj.source.upper() == 'CSV':
            file = os.path.join(
                executor.get_cube_path(),
                dimension_and_key.split('.')[0] + '.csv',
            )
            # with extension or not
            if not os.path.isfile(file):
                file.replace('.csv', '')
            df = pd.read_csv(file, sep=sep)
        else:
            db = MyDB(
                executor.database_config,
                db_name=executor.cube,
            )
            df = psql.read_sql_query(
                "SELECT * FROM {}".format(dimension_and_key.split('.')[0]),
                db.engine,
            )

        for dimension in cubes_obj.dimensions:
            if dimension_and_key.split('.')[0] == dimension.name:
                df.rename(columns=dimension.columns, inplace=True)

        fusion = fusion.merge(
            df,
            left_on=fact_key,
            right_on=dimension_and_key.split('.')[1],
            how='left',
            # remove suffixe from dimension and keep the same column name for facts
            suffixes=('', '_y'),
        )

    # measures in config-file only
    if cubes_obj.facts[0].measures:
        executor.measures = cubes_obj.facts[0].measures
    return fusion


def get_columns_n_tables(cubes_obj, executor):
    """
    Get all tables and their columns (and renames columns, if you specify this in the config file)
    :param cubes_obj: config file parser obj
    :param executor: MdxEngine instance
    :return:
    """

    all_columns = []
    tables = {}

    for table in cubes_obj.tables:

        tab = load_one_table(cubes_obj, executor, table.name)

        try:
            if table.columns:
                tab = tab[table.columns]

        except BaseException:
            print("table columns doesn't exist")
            print('pass with all columns')

        try:
            if table.new_names:
                tab = tab.rename(columns=table.new_names)

        except BaseException:
            print("verify your old and new columns names")
            print('pass with no change')

        all_columns += list(tab.columns)
        tables.update({table.name: tab})

    return all_columns, tables


# web client
def construct_web_star_schema_config_file(executor, cubes_obj, sep):
    """Construct star schema DataFrame from configuration file for web client.

    :param executor: MdxEngine instance
    :param cubes_obj: cubes parser object
    :param sep: csv file separator
    :return: star schema DataFrame
    """

    executor.facts = cubes_obj.facts[0].table_name

    fusion = load_one_table(
        cubes_obj,
        executor,
        executor.facts,
        sep
    )

    all_columns, tables = get_columns_n_tables(cubes_obj, executor)

    # load facts table columns
    if cubes_obj.facts[0].columns:
        all_columns += cubes_obj.facts[0].columns

    # measures in config-file only
    if cubes_obj.facts[0].measures:
        executor.measures = cubes_obj.facts[0].measures
        all_columns += cubes_obj.facts[0].measures

    for fact_key, dimension_and_key in cubes_obj.facts[0].keys.items():
        dimension_name = dimension_and_key.split('.')[0]
        if dimension_name in list(tables.keys()):
            df = tables[dimension_name]
        else:
            df = load_one_table(
                cubes_obj,
                executor,
                dimension_and_key.split('.')[0],
                sep
            )

        fusion = fusion.merge(
            df,
            left_on=fact_key,
            right_on=dimension_and_key.split('.')[1],
            how='left',
            # remove suffixe from dimension and keep the same column name for facts
            suffixes=('', '_y'),
        )

    return fusion[[column for column in all_columns if 'id' != column[-2:]]]
