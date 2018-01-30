# -*- encoding: utf8 -*-
"""
Do the same thing as MdxEngine, but with onle one file or database table (no need a star schema tables)
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from olapy.core.mdx.executor.execute import MdxEngine


class MdxEngineLite(MdxEngine):
    """The main class for executing a queries in one file.

    example of usage::

            olapy runserver -tf=/home/moddoy/olapy-data/cubes/sales/Facts.csv -c City,Licence,Amount,Count

    """
    def __init__(self):
        MdxEngine.__init__(self)

    def load_cube(self, table_or_file, sql_alchemy_uri=None, **kwargs):
        """
         After instantiating MdxEngine(), load_cube construct the cube and load all tables.

        :param table_or_file: full file path, or just database table name if sql_alchemy_uri provided
        :param sql_alchemy_uri: sql alchemy connection string
        :param measures: explicitly specify measures
        :param columns: explicitly specify columns, order matters
        :param sep: csv file separator
        """
        self.cube = table_or_file
        if sql_alchemy_uri:
            self.sql_alchemy = create_engine(sql_alchemy_uri)
        measures = kwargs.get('measures', None)
        sep = kwargs.get('sep', ';')
        columns = kwargs.get('columns', None)
        if self.sql_alchemy:
            self.tables_loaded = self.load_tables_db(columns)
        else:
            self.tables_loaded = self.load_tables_csv_files(sep, columns)
        if measures:
            self.measures = measures.split(',')
        else:
            self.measures = self.get_measures()
        if self.measures:
            # default measure is the first one
            self.selected_measures = [self.measures[0]]

        table_name = list(self.tables_loaded.keys())[0]
        self.star_schema_dataframe = self.tables_loaded[table_name]
        # remove measures from
        self.tables_loaded[table_name] = self.tables_loaded[table_name].drop(self.measures, axis=1)

    def get_measures(self):
        """
        :return: all numerical columns in Facts table.
        """

        table = list(self.tables_loaded.values())[0]
        not_id_columns = [column for column in table.columns if 'id' not in column]
        cleaned_facts = self.clean_data(table, not_id_columns)
        return [col for col in cleaned_facts.select_dtypes(include=[np.number], ).columns if col.lower()[-2:] != 'id']

    def load_tables_db(self, columns):
        """
        Load table from database.

        :param columns: list of columns names
        :return: tables dict with table name as key and dataframe as value
        """

        tables = {}
        print('Connection string = ' + str(self.sql_alchemy))
        results = self.sql_alchemy.execution_options(stream_results=True).execute('SELECT * FROM {}'.format(self.cube))
        # Fetch all the results of the query
        if columns:
            value = pd.DataFrame(iter(results), columns=results.keys())[columns.split(',')]
        else:
            value = pd.DataFrame(iter(results), columns=results.keys())  # Pass results as an iterator
        # with string_folding_wrapper we loose response time
        # value = pd.DataFrame(string_folding_wrapper(results),columns=results.keys())
        tables[self.cube] = value[
            [col for col in value.columns if col.lower()[-3:] != '_id']]

        return tables

    def load_tables_csv_files(self, sep, columns):
        """
        load the csv file
        :param sep: csv file separator
        :param columns: list of columns names
        :return: pandas DataFrame
        """
        tables = {}
        table_name = self.cube.split('/')[-1].replace('.csv', '')
        if columns:
            value = pd.read_csv(self.cube, sep=sep)[columns.split(',')]
        else:
            value = pd.read_csv(self.cube, sep=sep)

        tables[table_name] = value[[col for col in value.columns if col.lower()[-3:] != '_id']]

        return tables

    def get_cubes_names(self):
        return self.get_all_tables_names()

    def get_all_tables_names(self, **kwargs):
        if self.sql_alchemy:
            return [self.cube]
        else:
            return [self.cube.split('/')[-1].replace('.csv', '')]
