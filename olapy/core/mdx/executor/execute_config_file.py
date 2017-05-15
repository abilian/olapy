from __future__ import absolute_import, division, print_function

from ..tools.connection import MyDB
import pandas.io.sql as psql


def _load_table_config_file(executer_instance, cube_obj):
    """
    Load tables from config file.

    :param cube_obj: cubes object
    :return: tables dict with table name as key and DataFrame as value
    """
    tables = {}
    # just one facts table right now
    executer_instance.facts = cube_obj.facts[0].table_name

    db = MyDB(db=executer_instance.cube)

    for table in cube_obj.dimensions:
        value = psql.read_sql_query("SELECT * FROM {0}".format(table.name),
                                    db.connection)

        tables[table.name] = value[[
            col for col in value.columns if col.lower()[-3:] != '_id'
        ]]

    # update table display name
    for dimension in cube_obj.dimensions:
        if dimension.displayName and dimension.name and dimension.displayName != dimension.name:
            tables[dimension.displayName] = tables[dimension.name][
                dimension.columns]
            executer_instance.dimension_display_name.append(dimension.name)

    return tables


def _construct_star_schema_config_file(executer_instance, cubes_obj):
    """
    Construct star schema DataFrame from configuration file.

    :param cube_name:  cube name (or database name)
    :param cubes_obj: cubes object
    :return: star schema DataFrame
    """
    executer_instance.facts = cubes_obj.facts[0].table_name
    db = MyDB(db=executer_instance.cube)
    # load facts table
    fusion = psql.read_sql_query(
        "SELECT * FROM {0}".format(executer_instance.facts), db.connection)

    for fact_key, dimension_and_key in cubes_obj.facts[0].keys.items():
        df = psql.read_sql_query(
            "SELECT * FROM {0}".format(dimension_and_key.split('.')[0]),
            db.connection)

        fusion = fusion.merge(
            df, left_on=fact_key, right_on=dimension_and_key.split('.')[1])

        # TODO CHOSE BETWEEN THOSES DF
        # if separated dimensions
        # fusion = fusion.merge(df, left_on=fact_key,right_on=dimension_and_key.split('.')[1])

    # TODO CHOSE BETWEEN THOSES DF
    # if facts contains all dimensions
    # fusion = facts

    # measures in config-file only
    if cubes_obj.facts[0].measures:
        executer_instance.measures = cubes_obj.facts[0].measures

    return fusion


def _construct_web_star_schema_config_file(executer_instance, cubes_obj):
    """
    Construct star schema DataFrame from configuration file.

    :param cube_name:  cube name (or database name)
    :param cubes_obj: cubes object
    :return: star schema DataFrame
    """
    all_columns = []

    executer_instance.facts = cubes_obj.facts[0].table_name
    db = MyDB(db=executer_instance.cube)
    # load facts table

    # measures in config-file only
    if cubes_obj.facts[0].measures:
        executer_instance.measures = cubes_obj.facts[0].measures
        all_columns += cubes_obj.facts[0].measures

    if cubes_obj.facts[0].columns:
        all_columns += cubes_obj.facts[0].columns

    fusion = psql.read_sql_query(
        "SELECT * FROM {0}".format(executer_instance.facts), db.connection)

    tables = {}
    for table in cubes_obj.tables:

        tab = psql.read_sql_query("SELECT * FROM {0}".format(table.name),
                                  db.connection)

        try:
            if table.columns:
                tab = tab[table.columns]

        except:
            print("table columns doesn't exist")
            print('pass with all columns')

        try:
            if table.new_names:
                tab = tab.rename(columns=table.new_names)

        except:
            print("verify your old and new columns names")
            print('pass with no change')

        all_columns += list(tab.columns)
        tables.update({table.name: tab})

    for fact_key, dimension_and_key in cubes_obj.facts[0].keys.items():
        dimension_name = dimension_and_key.split('.')[0]
        if dimension_name in tables.keys():
            df = tables[dimension_name]
        else:
            df = psql.read_sql_query(
                "SELECT * FROM {0}".format(dimension_and_key.split('.')[0]),
                db.connection)

        fusion = fusion.merge(
            df, left_on=fact_key, right_on=dimension_and_key.split('.')[1])

    return fusion[[column for column in all_columns if 'id' != column[-2:]]]
