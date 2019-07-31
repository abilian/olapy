from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
from typing import Dict

import pandas as pd
import pandas.io.sql as psql

from .cube_loader import CubeLoader


class CubeLoaderCustom(CubeLoader):
    """Load Cube with some configurations."""

    def __init__(self, cube_config, cube_path=None, sqla_engine=None, sep=";"):
        CubeLoader.__init__(self)
        self.cube_config = cube_config
        self.cube_path = cube_path
        self.sep = sep
        self.sqla_engine = sqla_engine

    def load_one_table(self, table_name):
        if self.cube_config["source"].upper() == "CSV":
            facts = os.path.join(self.cube_path, table_name + ".csv")
            # with extension or not
            if not os.path.isfile(facts):
                facts.replace(".csv", "")
            table = pd.read_csv(facts, sep=self.sep)
        else:
            # load facts table
            table = psql.read_sql_query(
                "SELECT * FROM {}".format(table_name), self.sqla_engine
            )
        return table

    def load_tables(self):
        # type: () -> Dict[str, pd.DataFrame]
        """Load tables from config file.

        :return: tables dict with table name as key and DataFrame as value
        """

        tables = {}
        # just one facts table right now
        for dimension in self.cube_config["dimensions"]:

            df = self.load_one_table(dimension["name"])
            # change table display name
            if dimension["displayName"]:
                table_name = dimension["displayName"]
            else:
                table_name = dimension["name"]

            if "columns" in dimension and dimension["columns"]:
                try:
                    df = df[list(dimension["columns"].keys())]

                    # rename columns if value not None
                    df.rename(
                        columns=({k: v for k, v in dimension["columns"].items() if v}),
                        inplace=True,
                    )
                except KeyError:
                    pass

            tables[table_name] = df[
                [col for col in df.columns if col.lower()[-2:] != "id"]
            ]

        return tables

    def construct_star_schema(self, facts):
        """Construct star schema DataFrame from configuration file for excel
        client.

        :param facts:  Facts table name
        :return: star schema DataFrame
        """
        fusion = self.load_one_table(facts)
        for fact_key, dimension_and_key in self.cube_config["facts"]["keys"].items():
            if self.cube_config["source"].upper() == "CSV":
                file = os.path.join(
                    self.cube_path, dimension_and_key.split(".")[0] + ".csv"
                )
                # with extension or not
                if not os.path.isfile(file):
                    file.replace(".csv", "")
                df = pd.read_csv(file, sep=self.sep)
            else:
                df = psql.read_sql_query(
                    "SELECT * FROM {}".format(dimension_and_key.split(".")[0]),
                    self.sqla_engine,
                )

            for dimension in self.cube_config["dimensions"]:
                if dimension_and_key.split(".")[0] == dimension["name"]:
                    df.rename(columns=dimension["columns"], inplace=True)

            fusion = fusion.merge(
                df,
                left_on=fact_key,
                right_on=dimension_and_key.split(".")[1],
                how="left",
                # remove suffixe from dimension and keep the same column name for facts
                suffixes=("", "_y"),
            )
        return fusion

    def get_columns_n_tables(self):
        """Get all tables and their columns (and renames columns, if you
        specify this in the config file)

        :return:
        """

        all_columns = []
        tables = {}

        for table in self.cube_config["tables"]:

            df = self.load_one_table(table["name"])

            try:
                if table["columns"]:
                    df = df[table["columns"]]

            except KeyError:
                print("table columns doesn't exist")
                print("pass with all columns")

            try:
                if table["new_names"]:
                    df = df.rename(columns=table["new_names"])

            except KeyError:
                print("verify your old and new columns names")
                print("pass with no change")

            all_columns += list(df.columns)
            tables.update({table["name"]: df})

        return all_columns, tables

    # web client

    def construct_web_star_schema_config_file(self):
        """Construct star schema DataFrame from configuration file for web
        client.

        :return: star schema DataFrame
        """

        facts = self.cube_config["facts"]["table_name"]
        fusion = self.load_one_table(facts)
        all_columns, tables = self.get_columns_n_tables()

        # load facts table columns
        if self.cube_config["facts"]["columns"]:
            all_columns += self.cube_config["facts"]["columns"]

        # measures in config-file only
        if self.cube_config["facts"]["measures"]:
            all_columns += self.cube_config["facts"]["measures"]

        for fact_key, dimension_and_key in self.cube_config["facts"]["keys"]:
            dimension_name = dimension_and_key.split(".")[0]
            if dimension_name in list(tables.keys()):
                df = tables[dimension_name]
            else:
                df = self.load_one_table(dimension_and_key.split(".")[0])
            fusion = fusion.merge(
                df,
                left_on=fact_key,
                right_on=dimension_and_key.split(".")[1],
                how="left",
                # remove suffixe from dimension and keep the same column name for facts
                suffixes=("", "_y"),
            )

        return fusion[[column for column in all_columns if "id" != column[-2:]]]
