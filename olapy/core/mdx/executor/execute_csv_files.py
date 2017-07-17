from __future__ import absolute_import, division, print_function

import os

import pandas as pd

# split execution into three part (execute from config files,
# execute csv files if they respect olapy's start schema model,
# and execute data base tables if they respect olapy's start schema model)


def _load_tables_csv_files(executer_instance):
    """
    Load tables from csv files.

    :return: tables dict with table name as key and dataframe as value
    """
    tables = {}
    cube = executer_instance.get_cube()
    for file in os.listdir(cube):
        # to remove file extension ".csv"
        table_name = os.path.splitext(file)[0]
        value = pd.read_csv(
            os.path.join(cube, file), sep=executer_instance.sep)
        tables[table_name] = value[[
            col for col in value.columns if col.lower()[-3:] != '_id'
        ]]

    return tables


def _construct_star_schema_csv_files(executer_instance):
    """
    Construct star schema DataFrame from csv files.

    :param cube_name:  cube name (folder name)
    :return: star schema DataFrame
    """
    cube = executer_instance.get_cube()
    # loading facts table
    fusion = pd.read_csv(
        os.path.join(cube, executer_instance.facts + '.csv'),
        sep=executer_instance.sep)
    for file_name in os.listdir(cube):
        try:
            fusion = fusion.merge(
                pd.read_csv(
                    os.path.join(cube, file_name), sep=executer_instance.sep))
        except:
            print('No common column')
            pass

    return fusion
