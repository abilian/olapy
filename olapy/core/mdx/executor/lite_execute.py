# -*- encoding: utf8 -*-
"""
Do the same thing as MdxEngine, but with onle one file or database table
(no need for a star schema tables).
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import numpy as np
import pandas as pd

from ..executor import MdxEngine


class MdxEngineLite(MdxEngine):
    """The main class for executing a queries in one file.

    example of usage::

            olapy runserver -tf=/home/moddoy/olapy-data/cubes/sales/Facts.csv -c City,Licence,Amount,Count

    """

    def __init__(self,
                 direct_table_or_file,
                 columns=None,
                 measures=None,
                 sep=";",
                 **kwargs):
        MdxEngine.__init__(self, kwargs)
        self.cube = direct_table_or_file
        self.sep = sep
        self._columns = columns,
        if measures:
            self.measures = measures
        else:
            self.measures = self.get_measures()

    @property
    def columns(self):
        return self._columns[0].split(",")

    @columns.setter
    def columns(self, columns):
        if columns:
            self._columns = columns.split(",")
        else:
            self._columns = []

    def load_cube(self, table_or_file, **kwargs):
        """
        After instantiating MdxEngine(), load_cube construct the cube and load all tables.

        :param table_or_file: full file path, or just database table name if sql_alchemy_uri provided
        """
        self.cube = table_or_file
        if self.sqla_engine:
            self.tables_loaded = self.load_tables_from_db()
        else:
            self.tables_loaded = self.load_tables_from_csv_files()

        # self.selected_measures = [self.measures[0]]
        table_name = list(self.tables_loaded.keys())[0]
        self.star_schema_dataframe = self.tables_loaded[table_name]
        # remove measures from
        self.tables_loaded[table_name] = self.tables_loaded[table_name].drop(
            self.measures,
            axis=1,
        )

    def get_measures(self):
        """
        :return: all numerical columns in Facts table.
        """

        table = pd.read_csv(self.cube, sep=self.sep)
        not_id_columns = [
            column for column in table.columns if "id" not in column
        ]
        cleaned_facts = self.clean_data(table, not_id_columns)
        return [
            col
            for col in cleaned_facts.select_dtypes(include=[np.number]).columns
            if col.lower()[-2:] != "id"
        ]

    def load_tables_from_db(self):
        """
        Load table from database.

        :return: tables dict with table name as key and dataframe as value
        """

        tables = {}
        print("Connection string = " + str(self.sqla_engine.url))
        results = self.sqla_engine.execution_options(
            stream_results=True, ).execute(
                "SELECT * FROM {}".format(self.cube), )
        # Fetch all the results of the query
        if self.columns:
            value = pd.DataFrame(
                iter(results),
                columns=results.keys(),
            )[self.columns]
        else:
            value = pd.DataFrame(
                iter(results),
                columns=results.keys(),
            )  # Pass results as an iterator
        # with string_folding_wrapper we loose response time
        # value = pd.DataFrame(string_folding_wrapper(results),columns=results.keys())
        tables[self.cube] = value[[
            col for col in value.columns if col.lower()[-3:] != "_id"
        ]]

        return tables

    def load_tables_from_csv_files(self):
        """
        load the csv file

        :return: pandas DataFrame
        """
        tables = {}
        table_name = self.cube.split("/")[-1].replace(".csv", "")
        if self.columns:
            value = pd.read_csv(self.cube, sep=self.sep)[self.columns]
        else:
            value = pd.read_csv(self.cube, sep=self.sep)
        tables[table_name] = value[[
            col for col in value.columns if col.lower()[-3:] != "_id"
        ]]

        return tables

    def get_cubes_names(self):
        return [self.cube]
