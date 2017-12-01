# -*- encoding: utf8 -*-
"""
Olapy main's module, this module manipulate Mdx Queries and execute them.
Execution need two main objects:

    - table_loaded: which contains all tables needed to construct a cube
    - star_schema: which is the cube

Those two objects are constructed in three ways:

    - manually with a config file, see :mod:`execute_config_file`
    - automatically from csv files, if they respect olapy's \
    `start schema model <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_, see :mod:`execute_csv_files`
    - automatically from database, also if they respect the start schema model, see :mod:`execute_db`
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import itertools
import os
from collections import OrderedDict
from os.path import expanduser

import numpy as np
import pandas as pd

from olapy.core.mdx.parser.parse import Parser
from olapy.core.mdx.tools.olapy_config_file_parser import DbConfigParser
from ..tools.config_file_parser import ConfigParser
from ..tools.connection import MyDB
from .execute_config_file import construct_star_schema_config_file, \
    construct_web_star_schema_config_file, load_table_config_file
from .execute_csv_files import construct_star_schema_csv_files, \
    load_tables_csv_files
from .execute_db import construct_star_schema_db, load_tables_db

RUNNING_TOX = 'RUNNING_TOX' in os.environ
SUPPORTED_DATABASES = ['POSTGRES', 'MYSQL', 'MSSQL', 'ORACLE', 'SQLITE']
SUPPORTED_FILES = ['CSV']


def get_default_cube_directory():
    # toxworkdir does not expanduser properly under tox
    if 'OLAPY_PATH' in os.environ:
        home_directory = os.environ.get('OLAPY_PATH')
    # elif DATA_FOLDER is not None:
    #     home_directory = DATA_FOLDER
    elif RUNNING_TOX:
        home_directory = os.environ.get('HOME_DIR')
    else:
        home_directory = expanduser("~")

    if 'olapy-data' not in home_directory:
        home_directory = os.path.join(home_directory, 'olapy-data')

    return home_directory


class MdxEngine(object):
    """The main class for executing a query.

    :param cube_name: It must be under ~/olapy-data/cubes/cube_name.

        example cube_name = sales

        the full path -> *home_directory/olapy-data/cubes/sales*
    :param client_type: excel | web , by default excel, so you can use olapy as xmla server with excel spreadsheet,
        web if you want to use olapy with `olapy-web <https://github.com/abilian/olapy-web>`_,
    :param cubes_path: Olapy cubes path, which is under olapy-data,
        by default *~/olapy-data/cube_name*
    :param mdx_query: mdx query to execute
    :param olapy_data_location: By default *~/olapy-data/*
    :param sep: separator used in csv files
    :param fact_table_name: facts table name, Default **Facts**
    """

    # class variable , because spyne application = Application([XmlaProviderService],... throw exception if XmlaProviderService()
    # ----
    # DATA_FOLDER useful for olapy web (flask instance_path)
    # get olapy-data path with instance_path instead of 'expanduser'
    # DATA_FOLDER = None
    CUBE_FOLDER_NAME = "cubes"
    # (before instantiate MdxEngine I need to access cubes information)
    csv_files_cubes = []
    from_db_cubes = []
    olapy_data_location = get_default_cube_directory()
    cube_path = os.path.join(olapy_data_location, CUBE_FOLDER_NAME)
    source_type = 'csv'
    db_config = DbConfigParser(os.path.join(olapy_data_location, 'olapy-config'))
    cube_config_file_parser = ConfigParser(cube_path)
    mdx_parser = Parser()

    def __init__(
            self,
            cube_name,
            client_type='excel',
            cubes_path=None,
            mdx_query=None,
            olapy_data_location=None,
            sep=';',
            fact_table_name="Facts",
            database_config=db_config,
            cube_config=cube_config_file_parser,
            parser=mdx_parser):

        self.cube = cube_name
        self.sep = sep
        self.facts = fact_table_name
        self.parser = parser
        self._mdx_query = mdx_query
        if olapy_data_location is None:
            self.olapy_data_location = MdxEngine.olapy_data_location
        else:
            self.olapy_data_location = olapy_data_location
            MdxEngine.olapy_data_location = olapy_data_location
            MdxEngine.db_config = DbConfigParser(os.path.join(olapy_data_location, 'olapy-config'))
        if cubes_path is None:
            self.cube_path = MdxEngine.cube_path
        else:
            self.cube_path = cubes_path
            MdxEngine.cube_path = cubes_path
            MdxEngine.cube_config_file_parser = ConfigParser(cubes_path)

        self.database_config = database_config
        self.cube_config = cube_config
        # to get cubes from db
        self._ = self.get_cubes_names()
        self.client = client_type
        self.tables_loaded = self.load_tables()
        # all measures
        self.measures = self.get_measures()
        self.load_star_schema_dataframe = self.get_star_schema_dataframe()
        self.tables_names = self._get_tables_name()
        # default measure is the first one
        self.selected_measures = [self.measures[0]]

    @property
    def mdx_query(self):
        return self._mdx_query

    @mdx_query.setter
    def mdx_query(self, value):
        clean_query = value.strip().replace('\n', '').replace('\t', '')
        self.parser.mdx_query = clean_query
        self._mdx_query = clean_query

    @classmethod
    def _get_db_cubes_names(cls):
        """
        Get databases cubes names

        :return:
        """
        # get databases names first , and them instantiate MdxEngine with this database, thus \
        # MdxEngine will try to construct the star schema either automatically or manually

        # surrounded with try, except and pass so we continue getting cubes
        # from different sources (db, csv...) without interruption
        # try:
        db = MyDB(cls.db_config)
        if db.dbms.upper() == 'ORACLE':
            # You can think of a mysql "database" as a schema/user in Oracle.
            # todo username
            MdxEngine.from_db_cubes = [db.username]
        elif db.dbms.upper() == 'SQLITE':
            available_tables = db.engine.execute('PRAGMA database_list;').fetchall()
            MdxEngine.from_db_cubes = [available_tables[0][-1].split('/')[-1]]
        else:
            all_db_query = cls._gen_all_databases_query(db.dbms)
            result = db.engine.execute(all_db_query)
            available_tables = result.fetchall()
            MdxEngine.from_db_cubes = [
                database[0] for database in available_tables if
                database[0] not in ['mysql', 'information_schema', 'performance_schema', 'sys']
            ]
        # except Exception:
        #     type, value, traceback = sys.exc_info()
        #     print(type)
        #     print(value)
        #     print_tb(traceback)
        #     print('no database connexion')
        #     pass

    @staticmethod
    def _get_csv_cubes_names(cubes_location):
        """
        Get csv folder names

        :param cubes_location: Olapy cubes path, which is under olapy-data, by default ~/olapy-data/cubes_location
        :return:
        """

        # get csv folders names first , and them instantiate MdxEngine with this database, thus \
        # MdxEngine will try to construct the star schema either automatically or manually

        # try:
        MdxEngine.csv_files_cubes = [
            file for file in os.listdir(cubes_location)
            if os.path.isdir(os.path.join(cubes_location, file))
        ]
        # except Exception:
        #     type, value, traceback = sys.exc_info()
        #     print('Error opening %s' % (value))
        #     print('no csv folders')
        #     pass

    @classmethod
    def get_cubes_names(cls):
        """
        list all cubes ( By default from csv folder only), you can explicitly specify csv folder and databases
        with *MdxEngine.source_type = ('csv','db')*

        :return: list of all cubes
        """

        # by default , and before passing values to class with olapy runserver .... it executes this with csv
        # todo fix
        if 'csv' in cls.source_type and os.path.exists(cls.cube_path):
            MdxEngine._get_csv_cubes_names(cls.cube_path)
        else:
            MdxEngine.csv_files_cubes = []
        if 'db' in cls.source_type:
            MdxEngine._get_db_cubes_names()
        else:
            MdxEngine.from_db_cubes = []

        return MdxEngine.csv_files_cubes + MdxEngine.from_db_cubes

    @classmethod
    def _gen_all_databases_query(cls, dbms):
        """
        Each dbms has different query to get user databases names
        :param dbms: postgres | mysql | oracle | mssql
        :return: sql query to fetch all databases
        """
        if dbms.upper() == 'POSTGRES':
            return 'SELECT datname FROM pg_database WHERE datistemplate = false;'
        elif dbms.upper() == 'MYSQL':
            return 'SHOW DATABASES'
        elif dbms.upper() == 'MSSQL':
            return "select name FROM sys.databases where name not in ('master','tempdb','model','msdb');"
        elif dbms.upper() == 'ORACLE':
            # You can think of a mysql "database" as a schema/user in Oracle.
            return 'select username from dba_users;'

    def _get_tables_name(self):
        """Get all tables names.

        :return: list tables names
        """
        return self.tables_loaded.keys()

    def load_tables(self):
        """Load all tables as dict of { Table_name : DataFrame } for the current cube instance.

        :return: dict with key as table name and DataFrame as value
        """
        # config_file_parser = ConfigParser(self.cube_path)
        tables = {}
        if self.client == 'excel' and self.cube_config.config_file_exist() \
                and self.cube in self.cube_config.get_cubes_names():
            # for web (config file) we need only star_schema_dataframes, not all tables
            for cubes in self.cube_config.construct_cubes():
                if cubes.source.upper() in SUPPORTED_FILES + SUPPORTED_DATABASES:
                    tables = load_table_config_file(self, cubes)

        elif self.cube in self.from_db_cubes:
            tables = load_tables_db(self)
            if not tables:
                raise Exception('unable to load tables, check that the datase is not empty')

        elif self.cube in self.csv_files_cubes:
            tables = load_tables_csv_files(self)
        return tables

    def get_measures(self):
        """:return: all numerical columns in Facts table."""

        # if web, get measures from config file
        # from postgres and oracle databases , all tables names are lowercase

        # update config file path IMPORTANT
        self.cube_config.cube_path = self.cube_path

        if self.client == 'web' and self.cube_config.config_file_exist():
            for cubes in self.cube_config.construct_cubes():
                if cubes.facts:
                    # update facts table name
                    self.facts = cubes.facts[0].table_name

                    # if measures are specified in the config file
                    if cubes.facts[0].measures:
                        return cubes.facts[0].measures

        # col.lower()[-2:] != 'id' to ignore any id column
        return [
            col
            for col in self.tables_loaded[self.facts].select_dtypes(
                include=[np.number],).columns if col.lower()[-2:] != 'id'
        ]

    def _construct_star_schema_from_config(self, config_file_parser):
        """
        There is two different configuration, one for excel 'cubes-config.xml', \
        and the other for the web 'web_cube_config.xml' (if you want to use olapy-web), they are a bit different.
        :param config_file_parser: star schema Dataframe
        :return:
        """
        fusion = None
        for cubes in config_file_parser.construct_cubes():
            if cubes.source.upper() in SUPPORTED_FILES + SUPPORTED_DATABASES:
                if self.client == 'web':
                    # todo clean!!!!!
                    if cubes.facts:
                        fusion = construct_web_star_schema_config_file(self, cubes)
                    # todo clean!!!!! # todo clean!!!!! # todo clean!!!!!
                    elif cubes.source.upper() in SUPPORTED_FILES and cubes.name in self.csv_files_cubes:
                        fusion = construct_star_schema_csv_files(self)
                    elif cubes.source.upper() in SUPPORTED_DATABASES and cubes.name in self.from_db_cubes:
                        fusion = construct_star_schema_db(self)
                else:
                    fusion = construct_star_schema_config_file(self, cubes)
        return fusion

    def get_star_schema_dataframe(self):
        """Merge all DataFrames as star schema.

        :return: star schema DataFrame
        """
        fusion = None
        # config_file_parser = ConfigParser(self.cube_path)
        if self.cube_config.config_file_exist() and self.cube in self.cube_config.get_cubes_names():
            fusion = self._construct_star_schema_from_config(self.cube_config)

        elif self.cube in self.from_db_cubes:
            fusion = construct_star_schema_db(self)

        elif self.cube in self.csv_files_cubes:
            fusion = construct_star_schema_csv_files(self)

        return fusion[[
            col for col in fusion.columns if col.lower()[-3:] != '_id'
        ]]

    def get_all_tables_names(self, ignore_fact=False):
        """
        Get list of tables names.

        :param ignore_fact: return all table name with or without facts table name
        :return: all tables names
        """
        if ignore_fact:
            return [tab for tab in self.tables_names if self.facts not in tab]
        return self.tables_names

    def get_cube_path(self):
        """
        Get path to the cube ( ~/olapy-data/cubes ).

        :return: path to the cube
        """
        return os.path.join(self.cube_path, self.cube)

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
        """Get used dimensions and columns in the MDX Query (useful for DataFrame -> xmla response transformation).

        :param tuple_as_list: list of tuples

        example ::

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
                if tupl[0].upper() == 'MEASURES':
                    if tupl[-1] not in measures:
                        measures.append(tupl[-1])
                        tables_columns.update({self.facts: measures})
                    else:
                        continue
                else:
                    tables_columns.update({
                        tupl[0]:
                            self.tables_loaded[tupl[0]].columns[:len(
                                tupl[2:None if self.parser.hierarchized_tuples() else -1], )],
                    })

            axes.update({axis: tables_columns})
        return axes

    def execute_one_tuple(self, tuple_as_list, Dataframe_in, columns_to_keep):
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
        :param Dataframe_in: DataFrame in with you want to execute tuple
        :param columns_to_keep: (useful for executing many tuples, for instance execute_mdx)
            other columns to keep in the execution except the current tuple
        :return: Filtered DataFrame
        """
        df = Dataframe_in
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
        If we have multiple dimensions, with many columns like:

            columns_to_keep :

                Geo  -> Continent,Country

                Prod -> Company

                Time -> Year,Month,Day


        we have to use only dimension's columns of current dimension that exist
        in tuple_as_list and keep other dimensions columns

        so if tuple_as_list = ['Geography','Geography','Continent']

        columns_to_keep will be:

            columns_to_keep :

                Geo  -> Continent

                Prod -> Company

                Time -> Year,Month,Day


        we need columns_to_keep for grouping our columns in the DataFrame

        :param tuple_as_list:  example::

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
        if len(tuple_as_list) == 3 \
                and tuple_as_list[-1] in self.tables_loaded[tuple_as_list[0]].columns:
            # in case of [Geography].[Geography].[Country]
            cols = [tuple_as_list[-1]]
        else:
            cols = self.tables_loaded[tuple_as_list[0]].columns[:len(
                tuple_as_list[columns:],)]

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
        start_df = self.load_star_schema_dataframe
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
                start_df = df

            df_to_fusion.append(
                self.execute_one_tuple(
                    tupl,
                    start_df,
                    columns_to_keep.values(), ), )

        return df_to_fusion

    def fusion_dataframes(self, df_to_fusion):
        """
        Concat chunks of DataFrames.

        :param df_to_fusion: List of Pandas DataFrame.
        :return: Pandas DataFrame.
        """

        df = df_to_fusion[0]
        for next_df in df_to_fusion[1:]:
            df = pd.concat(self.add_missed_column(df, next_df))
        return df

    def check_nested_select(self):
        """
        Check if the MDX Query is Hierarchized and contains many tuples groups.

        :return: True | False
        """
        return not self.parser.hierarchized_tuples() and len(self.parser.get_nested_select()) >= 2

    def nested_tuples_to_dataframes(self, columns_to_keep):
        """
        Construct DataFrame of many groups.

        :param columns_to_keep: :func:`columns_to_keep` (useful for executing many tuples, for instance execute_mdx).
        :return: Pandas DataFrame.
        """
        dfs = []
        grouped_tuples = self.parser.get_nested_select()
        for tuple_groupe in grouped_tuples:
            transformed_tuple_groups = []
            for tuple in self.parser.split_group(tuple_groupe):
                tuple = tuple.split('].[')
                tuple[0] = tuple[0].replace('[', '')
                tuple[-1] = tuple[-1].replace(']', '')
                if tuple[0].upper() != 'MEASURES':
                    transformed_tuple_groups.append(tuple)

            dfs.append(self.tuples_to_dataframes(transformed_tuple_groups, columns_to_keep)[0])

        return dfs

    def execute_mdx(self, mdx_query):
        """Execute an MDX Query.

        Usage ::

            executor = MdxEngine('sales')
            query = "SELECT  FROM [sales] WHERE ([Measures].[Amount])"
            executor.execute_mdx(query)

        :return: dict with DataFrame execution result and (dimension and columns used as dict)

        example::

            {
            'result' : DataFrame result
            'columns_desc' : dict of dimension and columns used
            }

        """

        # todo temp  self.mdx_query is used in many places
        self.mdx_query = mdx_query

        # use measures that exists on where or insides axes
        query_axes = self.parser.decorticate_query(mdx_query)
        if self.change_measures(query_axes['all']):
            self.selected_measures = self.change_measures(query_axes['all'])

        tables_n_columns = self.get_tables_and_columns(query_axes)

        columns_to_keep = OrderedDict(
            (table, columns)
            for table, columns in tables_n_columns['all'].items()
            if table != self.facts)

        tuples_on_mdx_query = [
            tup for tup in query_axes['all'] if tup[0].upper() != 'MEASURES'
        ]

        if not self.parser.hierarchized_tuples():
            tuples_on_mdx_query = self._uniquefy_tuples(tuples_on_mdx_query)
            tuples_on_mdx_query.sort(key=lambda x: x[0])

        # if we have tuples in axes
        # to avoid prob with query like this: SELECT  FROM [Sales] WHERE ([Measures].[Amount])
        if tuples_on_mdx_query:

            if self.check_nested_select():
                df_to_fusion = self.nested_tuples_to_dataframes(columns_to_keep)
            else:
                df_to_fusion = self.tuples_to_dataframes(tuples_on_mdx_query, columns_to_keep)

            df = self.fusion_dataframes(df_to_fusion)

            cols = list(itertools.chain.from_iterable(columns_to_keep.values()))

            sort = self.parser.hierarchized_tuples()
            # margins=True for columns total !!!!!
            return {
                'result':
                    df.groupby(cols, sort=sort).sum()[self.selected_measures],
                'columns_desc':
                    tables_n_columns,
            }

        else:
            return {
                'result': self.load_star_schema_dataframe[self.selected_measures].sum().to_frame().T,
                'columns_desc': tables_n_columns,
            }
