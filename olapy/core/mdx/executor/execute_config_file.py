from __future__ import absolute_import, division, print_function

from ..tools.mem_bench import memory_usage
from ..tools.connection import MyDB
import pandas.io.sql as psql
import os


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

    memory_usage("1 - before executing query //// _load_table_config_file")
    for table in cube_obj.dimensions:
        with db.engine as connection:
            value = psql.read_sql_query("SELECT * FROM {0}".format(table.name),
                                        connection)

        tables[table.name] = value[[
            col for col in value.columns if col.lower()[-3:] != '_id'
        ]]

    memory_usage("2 - after query, before fetchall  /////// _load_table_config_file")
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

    memory_usage("1 - before executing query //// _construct_star_schema_config_file")
    with db.engine as connection:
        fusion = psql.read_sql_query(
            "SELECT * FROM {0}".format(executer_instance.facts), connection)

        for fact_key, dimension_and_key in cubes_obj.facts[0].keys.items():
            df = psql.read_sql_query(
                "SELECT * FROM {0}".format(dimension_and_key.split('.')[0]),
                connection)

            fusion = fusion.merge(
                df, left_on=fact_key, right_on=dimension_and_key.split('.')[1])


    memory_usage("2 - after query, before fetchall  /////// _construct_star_schema_config_file")
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
    db = MyDB(db=executer_instance.cube,db_config_file_path=os.path.dirname(executer_instance.cube_path))
    # load facts table

    if cubes_obj.facts[0].columns:
        all_columns += cubes_obj.facts[0].columns


    memory_usage("1 - before executing query //// 1111 _construct_web_star_schema_config_file ")
    fusion = psql.read_sql_query(
        "SELECT * FROM {0}".format(executer_instance.facts), db.engine)

    memory_usage("2 - after query, before fetchall  /////// 222222222222 _construct_star_schema_config_file")


    tables = {}
    memory_usage("1 - before executing query //// 3333333333 _construct_web_star_schema_config_file ")
    for table in cubes_obj.tables:

        tab = psql.read_sql_query("SELECT * FROM {0}".format(table.name),
                                  db.engine)

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

    memory_usage("2 - after query, before fetchall  /////// 44444444 _construct_star_schema_config_file")

    # measures in config-file only
    if cubes_obj.facts[0].measures:
        executer_instance.measures = cubes_obj.facts[0].measures
        all_columns += cubes_obj.facts[0].measures

    memory_usage("1 - before executing query //// 55555555 _construct_web_star_schema_config_file ")
    for fact_key, dimension_and_key in cubes_obj.facts[0].keys.items():
        dimension_name = dimension_and_key.split('.')[0]
        if dimension_name in tables.keys():
            df = tables[dimension_name]
        else:
            df = psql.read_sql_query(
                "SELECT * FROM {0}".format(dimension_and_key.split('.')[0]),
                db.engine)

        # TODO check merge (how)
        fusion = fusion.merge(
            df, left_on=fact_key, right_on=dimension_and_key.split('.')[1], how='left',
              # remove suffixe from dimension and keep the same column name for facts
              suffixes=('', '_y'))

    memory_usage("2 - after query, before fetchall  /////// 6666666666 _construct_star_schema_config_file")

    return fusion[[column for column in all_columns if 'id' != column[-2:]]]

