import numpy as np


def _clean(dataframes):
    """
    remove *_id columns
    :param dataframes:
    :return:
    """
    dfs = {}
    for df_name in dataframes.keys():
        dfs[df_name] = dataframes[df_name][
            [col for col in dataframes[df_name].columns if col.lower()[-3:] != "_id"]
        ]
    return dfs


def _get_measures(dataframes, mdx_engine):
    """:return: all numerical columns in Facts table."""

    # from postgres and oracle databases , all tables names are lowercase
    # col.lower()[-2:] != 'id' to ignore any id column
    facts_df = dataframes[mdx_engine.facts]
    not_id_columns = [
        column for column in facts_df.columns
        if "id" not in column
    ]
    cleaned_facts = mdx_engine.clean_data(
        facts_df,
        not_id_columns,
    )
    return [
        col
        for col in cleaned_facts.select_dtypes(include=[np.number],
                                               ).columns
        if col.lower()[-2:] != "id"
    ]


def _get_star_schema_dataframe(dataframes, mdx_engine):
    """
    Merge all DataFrames as star schema.

    :param sep: csv files separator.
    :param with_id_columns: start schema dataFrame contains id columns or not
    :return: star schema DataFrame
    """

    fusion = dataframes[mdx_engine.facts]
    for df in dataframes.values():
        try:
            fusion = fusion.merge(df)
        except BaseException:
            print("No common column")
            pass

    star_schema_df = mdx_engine.clean_data(fusion, mdx_engine.measures)

    return star_schema_df[[
        col for col in fusion.columns if col.lower()[-3:] != "_id"
    ]]


def patch_mdx_engine(mdx_engine, dataframes, facts_table_name='Facts', cube_name='sales'):
    mdx_engine.csv_files_cubes.append(cube_name)

    mdx_engine.cube = cube_name
    mdx_engine.facts = facts_table_name
    # load cubes names
    mdx_engine.source_type = ""

    # load tables
    mdx_engine.tables_loaded = _clean(dataframes)

    mdx_engine.measures = _get_measures(dataframes, mdx_engine)

    if mdx_engine.measures:
        # default measure is the first one
        mdx_engine.selected_measures = [mdx_engine.measures[0]]
    # construct star_schema
    if mdx_engine.tables_loaded:
        mdx_engine.star_schema_dataframe = _get_star_schema_dataframe(dataframes, mdx_engine)
