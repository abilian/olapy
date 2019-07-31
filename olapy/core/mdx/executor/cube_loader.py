from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
from typing import Dict, Text

import pandas as pd
from pandas.errors import MergeError


class CubeLoader(object):
    def __init__(self, cube_path=None, sep=";"):
        self.cube_path = cube_path
        self.sep = sep

    def load_tables(self):
        # type: () -> Dict[Text, pd.DataFrame]
        """Load tables from csv files.

        :return: tables dict with table name as key and dataframe as value
        """
        tables = {}
        for file in os.listdir(self.cube_path):
            # to remove file extension ".csv"
            table_name = os.path.splitext(file)[0]
            value = pd.read_csv(os.path.join(self.cube_path, file), sep=self.sep)
            tables[table_name] = value[
                [col for col in value.columns if col.lower()[-3:] != "_id"]
            ]
        return tables

    def construct_star_schema(self, facts):
        """Construct star schema DataFrame from csv files.

        :param facts: Facts table name
        :return: star schema DataFrame
        """
        # loading facts table
        df = pd.read_csv(os.path.join(self.cube_path, facts + ".csv"), sep=self.sep)
        for file_name in os.listdir(self.cube_path):
            try:
                df = df.merge(
                    pd.read_csv(os.path.join(self.cube_path, file_name), sep=self.sep)
                )
            except MergeError:
                print("No common column")

        return df
