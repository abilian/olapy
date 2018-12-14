# -*- encoding: utf8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import itertools
from collections import OrderedDict

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from typing import List

from ..execute import MdxEngine


class SparkMdxEngine(MdxEngine):

    def get_measures(self):
        """:return: all numerical columns in Facts table."""

        # from postgres and oracle databases , all tables names are lowercase
        # col.lower()[-2:] != 'id' to ignore any id column
        if self.facts in list(self.tables_loaded.keys()):
            return [
                col[0] for col in self.tables_loaded[self.facts].dtypes
                if col[0].lower()[-2:] != "id" and not col[1].startswith('string')
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
                if star_schema_df.select(measure).dtypes[0][1] == 'string':
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
            # df = df[(df[tuple_as_list[-1]].notnull())]
            df = df.where(df[tuple_as_list[-1]].isNotNull())

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
                df_with_less_columns = df_with_less_columns.withColumn(
                    missed_column, lit(-1))

        # with spark, column order must be the same
        return [
            df_with_less_columns.select(df_with_more_columns.columns),
            df_with_more_columns
        ]

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
                    # df = pd.concat(self.add_missed_column(df, next_df))
                    df, next_df = self.add_missed_column(df, next_df)
                    df = df.union(next_df)

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
            # df = pd.concat(self.add_missed_column(df, next_df), sort=False)
            df, next_df = self.add_missed_column(df, next_df)
            df = df.union(next_df)
        return df

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
            # result = df.groupby(cols, sort=sort).sum()[self.selected_measures]
            if sort:
                result = df.groupby(cols).sum().sort(cols)
                # result = sorted(df.groupby(cols).sum())
                # df.groupby(cols).sum().sort()
            else:
                result = df.groupby(cols).sum()

        else:
            # result = self.star_schema_dataframe[
            #     self.selected_measures].sum().to_frame().T
            result = self.star_schema_dataframe[
                self.selected_measures].groupBy().sum().select(
                ['sum(' + mes + ')' for mes in self.selected_measures])
        return {"result": result, "columns_desc": tables_n_columns}
