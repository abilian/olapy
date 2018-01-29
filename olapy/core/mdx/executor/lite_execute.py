import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from olapy.core.mdx.executor.execute import MdxEngine


class MdxEngineLite(MdxEngine):
    def __init__(self):
        MdxEngine.__init__(self)

    def load_cube(self, table_or_file, sql_alchemy_uri=None, **kwargs):
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
            self.measures = measures
        else:
            self.measures = self.get_measures()
        if self.measures:
            # default measure is the first one
            self.selected_measures = [self.measures[0]]

        # construct star_schema
        self.star_schema_dataframe = list(self.tables_loaded.values())[0]

    def get_measures(self):
        table = list(self.tables_loaded.values())[0]
        not_id_columns = [column for column in table.columns if 'id' not in column]
        cleaned_facts = self.clean_data(table, not_id_columns)
        return [col for col in cleaned_facts.select_dtypes(include=[np.number], ).columns if col.lower()[-2:] != 'id']

    def load_tables_db(self, columns):
        """
        Load tables from database.

        :param self: MdxEngine instance
        :return: tables dict with table name as key and dataframe as value
        """

        tables = {}
        print('Connection string = ' + str(self.sql_alchemy))
        results = self.sql_alchemy.execution_options(stream_results=True).execute('SELECT * FROM {}'.format(self.cube))
        # Fetch all the results of the query
        if columns:
            value = pd.DataFrame(iter(results), columns=results.keys())[columns]
        else:
            value = pd.DataFrame(iter(results), columns=results.keys())  # Pass results as an iterator
        # with string_folding_wrapper we loose response time
        # value = pd.DataFrame(string_folding_wrapper(results),columns=results.keys())
        tables[self.cube] = value[[col for col in value.columns if col.lower()[-3:] != '_id']]

        return tables

    def load_tables_csv_files(self, sep, columns):
        """
        Load tables from csv files.

        :param self: MdxEngine instance
        :param sep: csv file separator
        :return: tables dict with table name as key and dataframe as value
        """

        tables = {}
        table_name = self.cube.split('/')[-1].replace('.csv', '')
        if columns:
            value = pd.read_csv(self.cube, sep=sep)[columns]
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
