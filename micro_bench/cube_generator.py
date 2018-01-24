from __future__ import absolute_import, division, print_function

import os
import shutil
import string
from os.path import expanduser

import numpy as np
import pandas as pd

CUBE_NAME = "temp_cube"


class CubeGen:
    """Benchmark olapy query execution.

    :param number_dimensions: number of dimensions to generate (not including fact)
    :param rows_length: number of line in each dimension
    :param columns_length: cumber of columns in each dimension
    """

    # We have to generate DataFrames and save them to csv format because XmlaProviderService and
    # MdxEngine classes use those files

    def __init__(self, number_dimensions=1, rows_length=1000, columns_length=2):
        self.number_dimensions = number_dimensions
        self.rows_length = rows_length
        self.columns_length = columns_length
        self.cube_path = os.path.join(expanduser('~'), 'olapy-data', 'cubes')

    def generate_cube(self, min_val=5, max_val=100):
        """Generate dimension and fact that follows star schema.

        :param min_val: minimal value in every dimension
        :param max_val: maximal value in every dimension
        :return: dict of DataFrames
        """
        tables = {}
        facts = pd.DataFrame()
        for idx, dim in enumerate(range(self.number_dimensions)):
            table_name = 'table' + str(idx)
            table_values = pd.DataFrame(
                np.random.randint(
                    min_val,
                    max_val,
                    size=(self.rows_length, self.columns_length)),
                columns=list(
                    table_name + col
                    for col in string.ascii_uppercase[:self.columns_length]))
            table_values.index.name = table_name + "_id"
            tables[table_name] = table_values.reset_index()
            facts[table_name + "_id"] = tables[table_name][table_name + "_id"]

        facts['Amount'] = np.random.randint(
            300, 1000, size=(self.rows_length, 1))
        tables['Facts'] = facts
        return tables

    def generate_csv(self, tables):
        """Generate csv files for the generated DataFrames.

        :param tables: dict of DataFrames
        """

        if not os.path.isdir(os.path.join(self.cube_path, CUBE_NAME)):
            os.makedirs(os.path.join(self.cube_path, CUBE_NAME))
        cube_path = os.path.join(self.cube_path, CUBE_NAME)
        for (table_name, table_value) in tables.items():
            table_value.to_csv(
                os.path.join(os.path.join(cube_path, table_name + '.csv')),
                sep=";",
                index=False)

    def remove_temp_cube(self):
        """Remove the temporary cube."""

        if os.path.isdir(os.path.join(self.cube_path, CUBE_NAME)):
            shutil.rmtree(os.path.join(self.cube_path, CUBE_NAME))
