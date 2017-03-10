from __future__ import absolute_import, division, print_function

import os
import re
from collections import OrderedDict
import itertools

import numpy as np
import pandas as pd


class MdxEngine:
    """
    The principal class for executing a query

    :param cube_name: Cube name , it must be under cube_folder (example : olapy/cubes/sales)
    :param mdx_query: query to execute
    """
    CUBE_FOLDER = "cubes"
    FACTS_TABLE_NAME = "Facts"

    def __init__(self,
                 cube_name,
                 mdx_query=None,
                 cube_folder=CUBE_FOLDER,
                 sep=';',
                 fact_table_name=FACTS_TABLE_NAME):
        '''

        :param cube_folder: parent cube folder name
        :param mdx_query: query to execute
        :param sep: separator in the csv files
        '''
        self.cube = cube_name
        self.cube_folder = cube_folder
        self.sep = sep
        self.facts = fact_table_name
        self.mdx_query = mdx_query
        self.cube_path = self._get_cube_path()
        self.load_star_schema_dataframe = self._get_star_schema_dataframe(
            cube_name)
        self.tables_loaded = self._load_tables()
        self.tables_names = self._get_tables_name()
        self.measures = self._get_measures()

    @classmethod
    def get_cubes_names(self):
        '''
        :return: list cubes name under cubes folder
        '''

        location = os.path.join(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__), "..", "..", "..", "..")),
            MdxEngine.CUBE_FOLDER)
        return [
            file for file in os.listdir(location)
            if os.path.isdir(os.path.join(location, file))
        ]

    def _get_tables_name(self):
        return self.tables_loaded.keys()

    def _get_cube_path(self):
        '''
        :return: return local cube folder name with full path
        '''
        return os.path.join(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(__file__), '..', "..", '..', '..')),
            self.cube_folder)

    def _load_tables(self):
        """
        load all tables
        :return: dict with key as table name and DataFrame as value
        """
        cube = self.get_cube()
        tables = {}
        for file in os.listdir(cube):
            # to remove file extension ".csv"
            table_name = os.path.splitext(file)[0]
            value = pd.read_csv(os.path.join(cube, file), sep=self.sep)
            tables[table_name] = value[
                [col for col in value.columns if col.lower()[-3:] != '_id']]
        return tables

    def _get_measures(self):
        """

        :return: all numerical columns in facts table
        """
        return list(self.tables_loaded[self.facts].select_dtypes(
            include=[np.number]).columns)

    def _get_star_schema_dataframe(self, cube):
        '''
        :return: all DataFrames merged as star schema
        '''
        # star schema = (http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html)
        cube = self.get_cube()
        # loading facts table
        df = pd.read_csv(os.path.join(cube, self.facts + '.csv'), sep=self.sep)
        for f in os.listdir(cube):
            df = df.merge(pd.read_csv(os.path.join(cube, f), sep=self.sep))
        # TODO check this
        return df[[col for col in df.columns if col.lower()[-3:] != '_id']]

    def get_all_tables_names(self, ignore_fact=False):
        """
        get list of tables names of the cube

        :param ignore_fact: return all table name with facts table name
        :return: all tables names
        """
        if ignore_fact:
            return [tab for tab in self.tables_names if self.facts not in tab]
        return self.tables_names

    def get_cube(self):
        """
        get path to the cube (example /home/your_user_name/olapy-core/cubes)

        :return: path to the cube
        """
        return os.path.join(self.cube_path, self.cube)

    # TODO temporary function
    def decorticate_query(self, query):
        """
        get all tuples that exists in the MDX Query
        :param query: MDX Query
        :return: all tuples in the query
        """

        # TODO use grako instead and remove regex
        regex = "(\[[\w\d ]+\](\.\[[\w\d\.\- ]+\])*\.?((Members)|(\[Q\d\]))?)"
        # clean the query
        tuples_on_mdx_query = [[
            tup_att.replace('All ', '').replace('[', "").replace("]", "")
            for tup_att in tup[0].replace('.Members', '').split('.')
        ] for tup in re.compile(regex).findall(query)
                               if len(tup[0].split('.')) > 1]

        return tuples_on_mdx_query

    def change_measures(self, tuples_on_mdx):
        """
        set measures to which exists in the query

        :param tuples_on_where: list of tuples

        example : [ '[Measures].[Amount]' , '[Geography].[Geography].[Continent]' ]

        :return: measures columns names
        """
        return [
            tple[-1] for tple in tuples_on_mdx if tple[0].upper() == "MEASURES"
        ]

    def get_tables_and_columns(self, tuple_as_list):
        """
        get used dimensions and columns in the MDX Query (useful for DataFrame -> xmla response transformation)


        :param tuple_as_list: list of tuples

        example : [ '[Measures].[Amount]' , '[Geography].[Geography].[Continent]' ]

        :return: dimension and columns dict

        example :
        {
        Geography : ['Continent','Country'],
        Product : ['Company']
        Facts :  ['Amount','Count']
        }
        """
        tables_columns = OrderedDict()
        # TODO optimize
        measures = []
        for tupl in tuple_as_list:
            # if we have measures in columns or rows axes like :
            # SELECT {[Measures].[Amount],[Measures].[Count]} ON COLUMNS
            # we have to add measures directly to tables_columns
            if tupl[0].upper() == 'MEASURES':
                measures.append(tupl[-1])
                tables_columns.update({self.facts: measures})
            else:
                tables_columns.update({
                    tupl[0]:
                    self.tables_loaded[tupl[0]].columns[:len(tupl[2:])]
                })

        return tables_columns

    def execute_one_tuple(self, tuple_as_list, Dataframe_in, columns_to_keep):
        """
        filter a DataFrame (Dataframe_in) with one tuple

        Example :

        tuple = ['Geograpy','Geograpy','Continent','Europe','France','olapy']

        Dataframe_in in =

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
        #  tuple_as_list like ['Geograpy','Geograpy','Continent']
        #  return df with Continent column non empty
        if len(tuple_as_list) == 3:
            df = df[(df[tuple_as_list[-1]].notnull())]

        # tuple_as_list like['Geograpy', 'Geograpy', 'Continent' , 'America','US']
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

        return df[cols + self.measures]

    def add_missed_column(self, dataframe1, dataframe2):
        """
        solution to fix BUG : https://github.com/pandas-dev/pandas/issues/15525

        if you want to concat two dataframes with different columns like :

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


        :return: two DataFrames with same columns
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
        If we have multiple dimensions, with many columns like

        columns_to_keep =>

        ( Geo  -> Continent,Country
          Prod -> Company
          Time -> Year,Month,Day
        )

        we have to use only dimension's columns of current dimension that exist in tuple_as_list a keep other dimensions
        columns

        so if tuple_as_list = ['Geography','Geography','Continent']

        columns_to_keep will be

        columns_to_keep =>

        ( Geo  -> Continent
          Prod -> Company
          Time -> Year,Month,Day
        )

        (we need columns_to_keep for grouping our columns in the DataFrame)

        :param tuple_as_list: example -> ['Geography','Geography','Continent']
        :param columns_to_keep:  example -> { 'Geography' : ['Continent','Country'],
                                              'Time'      : ['Year','Month','Day']
                                            }
        :return: updated columns_to_keep
        """

        if len(tuple_as_list) == 3 and tuple_as_list[-1] in self.tables_loaded[
                tuple_as_list[0]].columns:
            # in case of [Geography].[Geography].[Country]
            cols = [tuple_as_list[-1]]
        else:
            cols = self.tables_loaded[tuple_as_list[0]].columns[:len(
                tuple_as_list[2:])]

        columns_to_keep.update({tuple_as_list[0]: cols})

    def execute_mdx(self):
        """
        execute and MDX Query

        usage ::

            executer = MdxEngine('sales')
            executer.mdx_query = "SELECT  FROM [sales] WHERE ([Measures].[Amount])"
            executer.execute_mdx()

        :return: dict with DataFrame execution result and (dimension and columns used as dict)

        {
        'result' : DataFrame result
        'columns_desc' : dict of dimension and columns used
        }

        """

        # use measures that exists on where or insides axes
        all_tuples = self.decorticate_query(self.mdx_query)

        if self.change_measures(all_tuples):
            self.measures = self.change_measures(all_tuples)

        # get only used columns and dimensions for all query
        start_df = self.load_star_schema_dataframe
        tables_n_columns = self.get_tables_and_columns(all_tuples)

        columns_to_keep = {
            table: columns
            for table, columns in tables_n_columns.items()
            if table != self.facts
        }

        # if we have measures on axes we have to ignore them
        tuples_on_mdx_query = [
            tup for tup in all_tuples if tup[0].upper() != 'MEASURES'
        ]
        # if we have tuples in axes
        # to avoid prob with query like this: SELECT  FROM [Sales] WHERE ([Measures].[Amount])
        if tuples_on_mdx_query:

            df_to_fusion = []
            table_name = tuples_on_mdx_query[0][0]
            # in every tuple
            for tupl in tuples_on_mdx_query:

                # if we have measures in columns or rows axes like :
                # SELECT {[Measures].[Amount],[Measures].[Count], [Customers].[Geography].[All Regions]} ON COLUMNS
                # we use only used columns for dimension in that tuple and keep other dimension's columns
                self.update_columns_to_keep(tupl, columns_to_keep)

                # a tuple with new dimension
                if tupl[0] != table_name:
                    # if we change dimension , we have to work on the exection's result on previous DataFrames

                    # TODO BUG !!! https://github.com/pandas-dev/pandas/issues/15525
                    # solution 1 .astype(str) ( take a lot of time from execution)
                    # solution 2 a['ccc'] = "" ( good solution i think ) also it avoid nan values and -1 :D !!
                    # solution 3 a['ccc'] = -1
                    # solution 4 finding something with merge

                    # fix 3 test
                    df = df_to_fusion[0]
                    for next_df in df_to_fusion[1:]:
                        df = pd.concat(self.add_missed_column(df, next_df))
                    # df = pd.concat(df_to_fusion)

                    table_name = tupl[0]
                    df_to_fusion = []
                    start_df = df

                df_to_fusion.append(
                    self.execute_one_tuple(tupl, start_df,
                                           columns_to_keep.values()))

            cols = list(
                itertools.chain.from_iterable(columns_to_keep.values()))

            # TODO BUG !!! https://github.com/pandas-dev/pandas/issues/15525
            # solution 1 .astype(str) ( take a lot of time from execution)
            # solution 2 a['ccc'] = "" ( good solution i think ) also it avoid nan values and -1 :D !!
            # solution 3 a['ccc'] = -1 (the best)
            # solution 4 finding something with merge

            # fix 3 test
            df = df_to_fusion[0]
            for next_df in df_to_fusion[1:]:
                df = pd.concat(self.add_missed_column(df, next_df))

            # TODO groupby in web demo (remove it for more performance)
            return {
                'result':
                df.drop_duplicates().replace(np.nan, -1).groupby(cols).sum(),
                'columns_desc': tables_n_columns
            }

        else:
            return {
                'result': start_df[self.measures].sum().to_frame().T,
                'columns_desc': tables_n_columns
            }
