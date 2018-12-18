# -*- encoding: utf8 -*-
"""
Olapy main's module, this module manipulate Mdx Queries and execute them.
Execution need two main objects:

    - table_loaded: which contains all tables needed to construct a cube
    - star_schema: which is the cube

Those two objects are constructed in three ways:

    - manually with a config file, see :mod:`cube_loader_custom`
    - automatically from csv files, if they respect olapy's
      `start schema model <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_,
      see :mod:`cube_loader`
    - automatically from database, also if they respect the start schema model, see :mod:`cube_loader_db`
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import itertools
import os
from collections import OrderedDict
from os.path import expanduser

from typing import List, Dict

import attr
import numpy as np
import pandas as pd
from pandas import DataFrame

from ..parser.parse import Parser

try:
    from ..tools.connection import get_dialect, get_dialect_name
    from .cube_loader_db import CubeLoaderDB
    from .cube_loader_custom import CubeLoaderCustom
except ImportError:
    pass


@attr.s
class MdxEngine(object):
    """The main class for executing a query.

    :param cube: It must be under ~/olapy-data/cubes/{cube_name}.

        example: if cube = 'sales'
        then full path -> *home_directory/olapy-data/cubes/sales*

    :param cubes_folder: which is under olapy-data, and contains all csv cubes by default * ~/olapy-data/cubes/...
    :param olapy_data_location: olapy-data path
    :param cube_config: cube-config.yml parsing file result (dict for creating customized cube)
    :param sql_engine: sql_alchemy engine if you don't want to use any database config file
    :param source_type: source data input, Default csv
    :param parser: mdx query parser
    :param facts:  facts table name, Default **Facts**

    """

    cube = attr.ib(default=None)
    facts = attr.ib(default="Facts")
    source_type = attr.ib(default="csv")
    parser = attr.ib(default=Parser())
    csv_files_cubes = attr.ib(default=attr.Factory(list))
    db_cubes = attr.ib(default=attr.Factory(list))
    sqla_engine = attr.ib(default=None)
    olapy_data_location = attr.ib(default=os.path.join(os.environ.get("OLAPY_PATH", expanduser("~")), "olapy-data"))
    cube_config = attr.ib(default=None)
    tables_loaded = attr.ib(default=None)
    star_schema_dataframe = attr.ib(default=None)
    measures = attr.ib(default=None)
    selected_measures = attr.ib(default=None)
    cubes_folder = attr.ib(default="cubes")

    # @olapy_data_location.default
    # def get_default_cubes_directory(self):
    #     # with OLAPY_PATH env var, we can inject olap_web's flask instance_path olapy-data,
    #     home_directory = os.environ.get("OLAPY_PATH", expanduser("~"))
    #     if "olapy-data" not in home_directory:
    #         home_directory = os.path.join(home_directory, "olapy-data")
    #
    #     return home_directory

    def _get_db_cubes_names(self):
        """
        Get databases cubes names.
        """
        # get databases names first , and them instantiate MdxEngine with this database, thus \
        # it will try to construct the star schema either automatically or manually

        dialect = get_dialect(self.sqla_engine)
        if not self.sqla_engine or str(self.sqla_engine) != str(dialect.engine):
            self.sqla_engine = dialect.engine
        return dialect.get_all_databases()

    @staticmethod
    def _get_csv_cubes_names(cubes_location):
        """
        Get csv folder names

        :param cubes_location: OlaPy cubes path, which is under olapy-data,
            by default ~/olapy-data/cubes/...
        """

        return [
            file for file in os.listdir(cubes_location)
            if os.path.isdir(os.path.join(cubes_location, file))
        ]

    def get_cubes_names(self):
        """
        List all cubes (by default from csv folder only).

        You can explicitly specify csv folder and databases
        with *self.source_type = ('csv','db')*

        :return: list of all cubes
        """

        if "db" in self.source_type:
            self.db_cubes = self._get_db_cubes_names()
        if "csv" in self.source_type:
            cubes_folder_path = os.path.join(
                self.olapy_data_location,
                self.cubes_folder,
            )

            self.csv_files_cubes = self._get_csv_cubes_names(cubes_folder_path)

        return self.db_cubes + self.csv_files_cubes

    def load_cube(self,
                  cube_name,
                  fact_table_name="Facts",
                  sep=";",
                  measures=None,
                  **kwargs):
        """
        After instantiating MdxEngine(), load_cube construct the cube
        and load all tables.

        :param cube_name: cube name
        :param fact_table_name:  facts table name, Default **Facts**
        :param sep: separator used in csv files
        :param measures: if you want to explicitly specify measures
        """
        self.cube = cube_name
        self.facts = fact_table_name
        # load cubes names
        self.get_cubes_names()  # necessary, it fills csv_files_cubes and db_cubes
        # load tables
        self.tables_loaded = self.load_tables(sep=sep)
        if measures:
            self.measures = measures
        else:
            self.measures = self.get_measures()
        if self.measures:
            # default measure is the first one
            self.selected_measures = [self.measures[0]]
        # construct star_schema
        if self.tables_loaded:
            self.star_schema_dataframe = self.get_star_schema_dataframe(sep=sep)

    def load_tables(self, sep):
        # type: (str) -> Dict[str, DataFrame]
        """
        Load all tables as dict of { Table_name : DataFrame } for the current
        cube instance.

        :param sep: csv files separator.
        :return: dict with table names as keys and DataFrames as values.
        """
        cubes_folder_path = self.get_cube_path()

        if (self.cube_config and self.cube_config["facts"] and self.cube == self.cube_config["name"]):
            cube_loader = CubeLoaderCustom(
                cube_config=self.cube_config,
                cube_path=cubes_folder_path,
                sqla_engine=self.sqla_engine,
                sep=sep,
            )
        elif self.cube in self.db_cubes:
            dialect_name = get_dialect_name(str(self.sqla_engine))
            if "postgres" in dialect_name:
                self.facts = self.facts.lower()
            cube_loader = CubeLoaderDB(self.sqla_engine)  # type: ignore
        # if not tables:
        #     raise Exception(
        #         'unable to load tables, check that the database is not empty',
        #     )
        # elif self.cube in self.csv_files_cubes:
        else:

            # force reimport CubeLoader every instance call (MdxEngine or SparkMdxEngine)
            from ..executor import CubeLoader
            cube_loader = CubeLoader(cubes_folder_path, sep)  # type: ignore
        return cube_loader.load_tables()

    def get_measures(self):
        """:return: all numerical columns in Facts table."""

        # from postgres and oracle databases , all tables names are lowercase
        # col.lower()[-2:] != 'id' to ignore any id column
        if self.facts in list(self.tables_loaded.keys()):
            not_id_columns = [
                column for column in self.tables_loaded[self.facts].columns
                if "id" not in column
            ]
            cleaned_facts = self.clean_data(
                self.tables_loaded[self.facts],
                not_id_columns,
            )
            return [
                col
                for col in cleaned_facts.select_dtypes(include=[np.number],
                                                       ).columns
                if col.lower()[-2:] != "id"
            ]

    @staticmethod
    def clean_data(star_schema_df, measures):
        """
        measure like this: 1 349 is not numeric so we try to transform it to 1349.

        :param star_schema_df: start schema dataframe
        :param measures: list of measures columns names

        :return: cleaned columns
        """
        if measures:
            for measure in measures:
                if star_schema_df[measure].dtype == object:
                    star_schema_df[measure] = star_schema_df[
                        measure].str.replace(
                        " ",
                        "",
                    )
                    try:
                        star_schema_df[measure] = star_schema_df[
                            measure].astype("float", )
                    except:
                        star_schema_df = star_schema_df.drop(measure, 1)
        return star_schema_df

    def get_star_schema_dataframe(self, sep, with_id_columns=False):
        """
        Merge all DataFrames as star schema.

        :param sep: csv files separator.
        :param with_id_columns: start schema dataFrame contains id columns or not
        :return: star schema DataFrame
        """
        # force reimport CubeLoader every instance call (MdxEngine or SparkMdxEngine)
        from ..executor import CubeLoader

        if (self.cube_config and self.cube_config["facts"] and self.cube == self.cube_config["name"]):
            self.facts = self.cube_config["facts"]["table_name"]
            # measures in config-file only
            if self.cube_config["facts"]["measures"]:
                self.measures = self.cube_config["facts"]["measures"]

            cube_path = self.get_cube_path()
            cube_loader = CubeLoaderCustom(
                cube_config=self.cube_config,
                cube_path=cube_path,
                sqla_engine=self.sqla_engine,
                sep=sep,
            )

        elif self.cube in self.db_cubes:
            cube_loader = CubeLoaderDB(self.sqla_engine)

        # elif self.cube in self.csv_files_cubes:
        else:
            cube_path = self.get_cube_path()
            cube_loader = CubeLoader(cube_path)

        fusion = cube_loader.construct_star_schema(self.facts)
        star_schema_df = self.clean_data(fusion, self.measures)

        if with_id_columns:
            return star_schema_df

        return star_schema_df[[
            col for col in fusion.columns if col.lower()[-3:] != "_id"
        ]]

    def get_all_tables_names(self, ignore_fact=False):
        """
        Get list of tables names.

        :param ignore_fact: return all table name with or without facts table name
        :return: all tables names
        """
        if ignore_fact:
            return [tab for tab in self.tables_loaded if self.facts not in tab]

        return list(self.tables_loaded)

    def get_cube_path(self):
        """
        Get path to the cube ( ~/olapy-data/cubes ).

        :return: path to the cube
        """
        if self.source_type.upper() == "CSV":
            return os.path.join(
                self.olapy_data_location,
                self.cubes_folder,
                self.cube,
            )
        return None

    @staticmethod
    def change_measures(tuples_on_mdx):
        """Set measures to which exists in the query.

        :param tuples_on_mdx: List of tuples

            example ::

             [ '[Measures].[Amount]' , '[Geography].[Geography].[Continent]' ]

        :return: measures column's names *(Amount for the example)*
        """

        list_measures = []
        for tple in tuples_on_mdx:
            if tple[0].upper() == "MEASURES" and tple[-1] not in list_measures:
                list_measures.append(tple[-1])

        return list_measures

    def get_tables_and_columns(self, tuple_as_list):
        """Get used dimensions and columns in the MDX Query.

        Useful for DataFrame -> xmla response transformation.

        :param tuple_as_list: list of tuples

        example::

            [
            '[Measures].[Amount]',
            '[Product].[Product].[Crazy Development]',
            '[Geography].[Geography].[Continent]'
            ]

        :return: dimension and columns dict

        example::

            {
            Geography : ['Continent','Country'],
            Product : ['Company']
            Facts :  ['Amount','Count']
            }
        """
        axes = {}
        for axis, tuples in tuple_as_list.items():
            measures = []
            tables_columns = OrderedDict()
            # if we have measures in columns or rows axes like :
            # SELECT {[Measures].[Amount],[Measures].[Count]} ON COLUMNS
            # we have to add measures directly to tables_columns
            for tupl in tuples:
                if tupl[0].upper() == "MEASURES":
                    if tupl[-1] not in measures:
                        measures.append(tupl[-1])
                        tables_columns.update({self.facts: measures})
                    else:
                        continue

                else:
                    tables_columns.update({
                        tupl[0]:
                            self.tables_loaded[tupl[0]].columns[
                            :len(tupl[2:None if self.parser.hierarchized_tuples() else -1])],
                    }, )

            axes.update({axis: tables_columns})
        return axes

    def execute_one_tuple(self, tuple_as_list, dataframe_in, columns_to_keep):
        """
        Filter a DataFrame (Dataframe_in) with one tuple.

        Example ::

            tuple = ['Geography','Geography','Continent','Europe','France','olapy']

            Dataframe_in in :

            +-------------+----------+---------+---------+---------+
            | Continent   | Country  | Company | Article | Amount  |
            +=============+==========+=========+=========+=========+
            | America     | US       | MS      | SSAS    | 35150   |
            +-------------+----------+---------+---------+---------+
            | Europe      |  France  | AB      | olapy   | 41239   |
            +-------------+----------+---------+---------+---------+
            |  .....      |  .....   | ......  | .....   | .....   |
            +-------------+----------+---------+---------+---------+

            out :

            +-------------+----------+---------+---------+---------+
            | Continent   | Country  | Company | Article | Amount  |
            +=============+==========+=========+=========+=========+
            | Europe      |  France  | AB      | olapy   | 41239   |
            +-------------+----------+---------+---------+---------+


        :param tuple_as_list: tuple as list
        :param dataframe_in: DataFrame in with you want to execute tuple
        :param columns_to_keep: (useful for executing many tuples, for instance execute_mdx)
            other columns to keep in the execution except the current tuple
        :return: Filtered DataFrame
        """
        df = dataframe_in
        #  tuple_as_list like ['Geography','Geography','Continent']
        #  return df with Continent column non empty
        if len(tuple_as_list) == 3:
            df = df[(df[tuple_as_list[-1]].notnull())]

        # tuple_as_list like['Geography', 'Geography', 'Continent' , 'America','US']
        # execute : df[(df['Continent'] == 'America')] and
        #           df[(df['Country'] == 'US')]
        elif len(tuple_as_list) > 3:
            for idx, tup_att in enumerate(tuple_as_list[3:]):
                # df[(df['Year'] == 2010)]
                # 2010 must be as int, otherwise , pandas generate exception
                if tup_att.isdigit():
                    tup_att = int(tup_att)

                df = df[(df[self.tables_loaded[tuple_as_list[0]].columns[idx]]
                         == tup_att)]
        cols = list(itertools.chain.from_iterable(columns_to_keep))
        return df[cols + self.selected_measures]

    @staticmethod
    def add_missed_column(dataframe1, dataframe2):
        """
        if you want to concat two Dataframes with different columns like :

        +-------------+---------+
        | Continent   | Amount  |
        +=============+=========+
        | America     | 35150   |
        +-------------+---------+
        | Europe      | 41239   |
        +-------------+---------+

        and :

        +-------------+---------------+---------+
        | Continent   | Country_code  | Amount  |
        +=============+===============+=========+
        | America     | 1111          | 35150   |
        +-------------+---------------+---------+

        result :

        +-------------+--------------+---------+
        | Continent   | Country_code | Amount  |
        +=============+==============+=========+
        | America     | 1111.0       |35150    |
        +-------------+--------------+---------+
        | Europe      | NaN          |41239    |
        +-------------+--------------+---------+

        Country_code is converted to float,

        so the solution is to add a column to the fist DataFrame filled with -1, thus

        +-------------+---------------+---------+
        | Continent   | Country_code  | Amount  |
        +=============+===============+=========+
        | America     | -1            | 35150   |
        +-------------+---------------+---------+
        | Europe      | -1            | 41239   |
        +-------------+---------------+---------+

        and :

        +-------------+---------------+---------+
        | Continent   | Country_code  | Amount  |
        +=============+===============+=========+
        | America     | 1111          | 35150   |
        +-------------+---------------+---------+

        result :

        +-------------+--------------+---------+
        | Continent   | Country_code | Amount  |
        +=============+==============+=========+
        | America     | 1111         |35150    |
        +-------------+--------------+---------+
        | Europe      | -1           |41239    |
        +-------------+--------------+---------+

        :param dataframe1: first DataFrame
        :param dataframe2: second DataFrame

        :return: Two DataFrames with same columns
        """
        df_with_less_columns = dataframe1
        df_with_more_columns = dataframe2
        if len(list(dataframe1.columns)) != len(list(dataframe2.columns)):
            if len(list(dataframe1.columns)) > len(list(dataframe2.columns)):
                df_with_more_columns = dataframe1
                df_with_less_columns = dataframe2
            missed_columns = [
                col for col in list(df_with_more_columns.columns)
                if col not in list(df_with_less_columns.columns)
            ]
            for missed_column in missed_columns:
                df_with_less_columns[missed_column] = -1

        return [df_with_less_columns, df_with_more_columns]

    def update_columns_to_keep(self, tuple_as_list, columns_to_keep):
        """
        If we have multiple dimensions, with many columns like::

            columns_to_keep :

                Geo  -> Continent,Country

                Prod -> Company

                Time -> Year,Month,Day

        we have to use only dimension's columns of current dimension that exist
        in tuple_as_list and keep other dimensions columns.

        So if tuple_as_list = ['Geography','Geography','Continent']

        then columns_to_keep will be::

            columns_to_keep :

                Geo  -> Continent

                Prod -> Company

                Time -> Year,Month,Day

        we need columns_to_keep for grouping our columns in the DataFrame

        :param tuple_as_list:

            example::

                ['Geography','Geography','Continent']

        :param columns_to_keep:

            example::

                {
                'Geography': ['Continent','Country'],
                'Time': ['Year','Month','Day']
                }

        :return: updated columns_to_keep
        """

        columns = 2 if self.parser.hierarchized_tuples() else 3
        if (len(tuple_as_list) == 3 and tuple_as_list[-1] in self.tables_loaded[tuple_as_list[0]].columns):
            # in case of [Geography].[Geography].[Country]
            cols = [tuple_as_list[-1]]
        else:
            cols = self.tables_loaded[tuple_as_list[0]].columns[:len(
                tuple_as_list[columns:], )]

        columns_to_keep.update({tuple_as_list[0]: cols})

    @staticmethod
    def _uniquefy_tuples(tuples):
        """
        Remove redundant tuples.

        :param tuples: list of string of tuples.
        :return: list of string of unique tuples.
        """
        uniquefy = []
        for tup in tuples:
            if tup not in uniquefy:
                uniquefy.append(tup)

        return uniquefy

    def tuples_to_dataframes(self, tuples_on_mdx_query, columns_to_keep):
        """
        Construct DataFrame of many groups mdx query.

        many groups mdx query is something like:

        example with 3 groups::

            SELECT{ ([A].[A].[A])
                    ([B].[B].[B])
                    ([C].[C].[C]) }
            FROM [D]

        :param tuples_on_mdx_query: list of string of tuples.
        :param columns_to_keep: (useful for executing many tuples, for instance execute_mdx).
        :return: Pandas DataFrame.
        """
        # get only used columns and dimensions for all query
        star_df = self.star_schema_dataframe
        df_to_fusion = []
        table_name = tuples_on_mdx_query[0][0]
        # in every tuple
        for tupl in tuples_on_mdx_query:
            # if we have measures in columns or rows axes like :
            # SELECT {[Measures].[Amount],[Measures].[Count], [Customers].[Geography].[All Regions]} ON COLUMNS
            # we use only used columns for dimension in that tuple and keep
            # other dimension's columns
            self.update_columns_to_keep(tupl, columns_to_keep)
            # a tuple with new dimension
            if tupl[0] != table_name:
                # if we change dimension , we have to work on the
                # exection's result on previous DataFrames

                df = df_to_fusion[0]
                for next_df in df_to_fusion[1:]:
                    df = pd.concat(self.add_missed_column(df, next_df))

                table_name = tupl[0]
                df_to_fusion = []
                star_df = df

            df_to_fusion.append(
                self.execute_one_tuple(
                    tupl,
                    star_df,
                    columns_to_keep.values(),
                ), )

        return df_to_fusion

    def fusion_dataframes(self, df_to_fusion):
        # type: (List[DataFrame]) -> DataFrame
        """Concat chunks of DataFrames.

        :param df_to_fusion: List of Pandas DataFrame.
        :return: a Pandas DataFrame.
        """

        df = df_to_fusion[0]
        for next_df in df_to_fusion[1:]:
            df = pd.concat(self.add_missed_column(df, next_df), sort=False)
        return df

    def check_nested_select(self):
        # type: () -> bool
        """
        Check if the MDX Query is Hierarchized and contains many tuples groups.
        """
        return (not self.parser.hierarchized_tuples() and len(self.parser.get_nested_select()) >= 2)

    def nested_tuples_to_dataframes(self, columns_to_keep):
        """
        Construct DataFrame of many groups.

        :param columns_to_keep: :func:`columns_to_keep`
            (useful for executing many tuples, for instance execute_mdx).
        :return: a list of Pandas DataFrame.
        """
        dfs = []
        grouped_tuples = self.parser.get_nested_select()
        for tuple_groupe in grouped_tuples:
            transformed_tuple_groups = []
            for tuple in self.parser.split_group(tuple_groupe):
                tuple = tuple.split("].[")
                tuple[0] = tuple[0].replace("[", "")
                tuple[-1] = tuple[-1].replace("]", "")
                if tuple[0].upper() != "MEASURES":
                    transformed_tuple_groups.append(tuple)

            dfs.append(
                self.tuples_to_dataframes(
                    transformed_tuple_groups,
                    columns_to_keep,
                )[0], )

        return dfs

    def clean_mdx_query(self, mdx_query):
        try:
            clean_query = mdx_query.decode("utf-8").strip().replace("\n", "").replace("\t", "")
        except AttributeError:
            clean_query = mdx_query.strip().replace("\n", "").replace("\t", "")
        # todo property in parser
        self.parser.mdx_query = clean_query
        return clean_query

    def execute_mdx(self, mdx_query):
        """Execute an MDX Query.

        Usage ::

            executor = MdxEngine()
            executor.load_cube('sales')
            query = "SELECT FROM [sales] WHERE ([Measures].[Amount])"
            executor.execute_mdx(query)

        :param mdx_query: Mdx Query

        :return: dict with DataFrame execution result and (dimension and columns used as dict)

        example::

            {
            'result': DataFrame result
            'columns_desc': dict of dimension and columns used
            }

        """
        query = self.clean_mdx_query(mdx_query)
        # use measures that exists on where or insides axes
        query_axes = self.parser.decorticate_query(query)
        if self.change_measures(query_axes["all"]):
            self.selected_measures = self.change_measures(query_axes["all"])

        tables_n_columns = self.get_tables_and_columns(query_axes)

        columns_to_keep = OrderedDict(
            (table, columns)
            for table, columns in tables_n_columns["all"].items()
            if table != self.facts)

        tuples_on_mdx_query = [
            tup for tup in query_axes["all"] if tup[0].upper() != "MEASURES"
        ]
        if not self.parser.hierarchized_tuples():
            tuples_on_mdx_query = self._uniquefy_tuples(tuples_on_mdx_query)
            tuples_on_mdx_query.sort(key=lambda x: x[0])

        # if we have tuples in axes
        # to avoid prob with query like this:
        # SELECT  FROM [Sales] WHERE ([Measures].[Amount])
        if tuples_on_mdx_query:

            if self.check_nested_select():
                df_to_fusion = self.nested_tuples_to_dataframes(
                    columns_to_keep, )
            else:
                df_to_fusion = self.tuples_to_dataframes(
                    tuples_on_mdx_query,
                    columns_to_keep,
                )

            df = self.fusion_dataframes(df_to_fusion)

            cols = list(
                itertools.chain.from_iterable(columns_to_keep.values(), ))

            sort = self.parser.hierarchized_tuples()
            # margins=True for columns total !!!!!
            result = df.groupby(cols, sort=sort).sum()[self.selected_measures]

        else:
            result = self.star_schema_dataframe[
                self.selected_measures].sum().to_frame().T

        return {"result": result, "columns_desc": tables_n_columns}
