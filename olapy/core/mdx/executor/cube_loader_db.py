from __future__ import absolute_import, division, print_function, \
    unicode_literals

from typing import Dict, Text

import pandas as pd
import pandas.io.sql as psql
from pandas import DataFrame
from sqlalchemy import inspect

from ..tools.connection import get_dialect_name
from . import CubeLoader


class CubeLoaderDB(CubeLoader):
    """
    Part of :mod:`execute.py` module, here olapy construct cube from DATABASE
    automatically based on
    `start schema model <http://datawarehouse4u.info/Data-warehouse-schema-architecture-star-schema.html>`_
    """

    def __init__(self, sqla_engine):
        CubeLoader.__init__(self)
        self.sqla_engine = sqla_engine

    def load_tables(self):
        # type: () -> Dict[Text, DataFrame]
        """
        Load tables from database.

        :return: tables dict with table name as key and dataframe as value
        """

        tables = {}
        print("Connection string = " + str(self.sqla_engine))
        inspector = inspect(self.sqla_engine)
        dialect_name = get_dialect_name(str(self.sqla_engine))
        for table_name in inspector.get_table_names():
            if "oracle" in dialect_name and table_name.upper() == "FACTS":
                table_name = table_name.title()
            results = self.sqla_engine.execution_options(
                stream_results=True, ).execute("SELECT * FROM {}".format(table_name), )
            # Fetch all the results of the query
            df = pd.DataFrame(
                iter(results),
                columns=results.keys(),
            )  # Pass results as an iterator
            # with string_folding_wrapper we loose response time
            # value = pd.DataFrame(string_folding_wrapper(results),columns=results.keys())
            tables[table_name] = df[[
                col for col in df.columns if col.lower()[-3:] != "_id"
            ]]

        return tables

    def construct_star_schema(self, facts):
        # type: (Text) -> DataFrame
        """
        Construct star schema DataFrame from database.

        :param facts: Facts table name
        :return: star schema DataFrame
        """

        df = psql.read_sql_query(
            "SELECT * FROM {}".format(facts),
            self.sqla_engine,
        )
        inspector = inspect(self.sqla_engine)

        for db_table_name in inspector.get_table_names():
            # try:
            #     db_table_name = str(db_table_name)
            # except Exception:
            #     if isinstance(db_table_name, Iterable):
            #         db_table_name = db_table_name[0]
            try:
                df = df.merge(
                    psql.read_sql_query(
                        "SELECT * FROM {}".format(db_table_name),
                        self.sqla_engine,
                    ), )
            except BaseException:
                print("No common column between {} and {}".format(
                    facts,
                    db_table_name,
                ))
                pass

        return df
