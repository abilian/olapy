# -*- encoding: utf8 -*-
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
from typing import Dict, Text

from pyspark.sql import DataFrame, SparkSession

from olapy.core.mdx.executor.cube_loader import CubeLoader

spark = SparkSession.builder.appName("olapy").getOrCreate()


class SparkCubeLoader(CubeLoader):
    def load_tables(self):
        # type: () -> Dict[Text, DataFrame]
        """Load tables from csv files.

        :return: tables dict with table name as key and dataframe as value
        """
        tables = {}
        for file in os.listdir(self.cube_path):
            # to remove file extension ".csv"
            table_name = os.path.splitext(file)[0]
            value = spark.read.csv(
                os.path.join(self.cube_path, file),
                header=True,
                sep=self.sep,
                inferSchema=True,
            )
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
        fusion = spark.read.csv(
            os.path.join(self.cube_path, facts + ".csv"),
            header=True,
            sep=self.sep,
            inferSchema=True,
        )
        for file_name in os.listdir(self.cube_path):
            try:
                df = spark.read.csv(
                    os.path.join(self.cube_path, file_name),
                    header=True,
                    sep=self.sep,
                    inferSchema=True,
                )
                common_columns = list(set(df.columns).intersection(fusion.columns))
                fusion = fusion.join(df, common_columns)
            except IndexError:
                print("No common column")

        return fusion
