"""To load DataFrames (csv files for instance), OlaPy uses some it own loaders
(cube_loader.py) modules, but when using pyodide, it's not possible to use them
due to some file system access constraints, so the idea is that Pyodide load
DataFrame with `pyodide.open_url <https://github.com/iodide-
project/pyodide/blob/master/docs/api_reference.md#pyodideopen_urlurl>`_ and
them injects these Df to OlaPy.

import pandas as pd
import pyodide
from olapy.core.services.xmla_lib import get_response

xmla_request_params = {'cube': 'sales','request_type': 'DISCOVER_PROPERTIES','properties': {},
          'restrictions': {'PropertyName': 'ServerName'},'mdx_query': None}

dataframes = {'Facts' : pd.read_csv(pyodide.open_url("olapy-data/cubes/sales/Facts.csv"),sep=';', encoding='utf8'),
'Product':pd.read_csv(pyodide.open_url("olapy-data/cubes/sales/Product.csv"),sep=';', encoding='utf8'),
'Geography':pd.read_csv(pyodide.open_url("olapy-data/cubes/sales/Geography.csv"),sep=';', encoding='utf8')
}

# get_response uses the patch
get_response(xmla_request_params=xmla_request_params,dataframes=dataframes, output='xmla') # or output='dict'
"""

from __future__ import absolute_import, division, print_function, \
    unicode_literals

import numpy as np
from pandas.errors import MergeError


def _clean(dataframes):
    """remove *_id columns.

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
    not_id_columns = [column for column in facts_df.columns if "id" not in column]
    cleaned_facts = mdx_engine.clean_data(facts_df, not_id_columns)
    return [
        col
        for col in cleaned_facts.select_dtypes(include=[np.number]).columns
        if col.lower()[-2:] != "id"
    ]


def _get_star_schema_dataframe(dataframes, mdx_engine):
    """Merge all DataFrames as star schema.

    :return: star schema DataFrame
    """

    fusion = dataframes[mdx_engine.facts]
    for df in dataframes.values():
        try:
            fusion = fusion.merge(df)
        except MergeError:
            print("No common column")

    star_schema_df = mdx_engine.clean_data(fusion, mdx_engine.measures)

    return star_schema_df[[col for col in fusion.columns if col.lower()[-3:] != "_id"]]


def inject_dataframes(
    mdx_engine, dataframes, facts_table_name="Facts", cube_name="sales"
):
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
        mdx_engine.star_schema_dataframe = _get_star_schema_dataframe(
            dataframes, mdx_engine
        )
