"""
Managing all database access
"""
from __future__ import absolute_import, division, print_function

from typing import List, Optional, Text

from sqlalchemy.engine import Engine


class Dialect(object):
    """Connect to sql database."""

    def __init__(self, sqla_engine=None):
        # type: (Optional[Engine]) -> None
        """
        Connect to cube from database

        :param sqla_engine: SqlAlchemy engine instance
        """

        self.engine = sqla_engine

    def gen_all_databases_query(self):
        # type: () -> Text
        """
        Each dbms has different query to get user databases names.

        :return: sql query to fetch all databases
        """
        raise NotImplementedError

    def get_all_databases(self):
        # type: () -> List[Text]
        all_db_query = self.gen_all_databases_query()
        result = self.engine.execute(all_db_query)  # type: ignore
        available_tables = result.fetchall()
        return [
            database[0] for database in available_tables
            if database[0] not in [
                "mysql",
                "information_schema",
                "performance_schema",
                "sys",
            ]
        ]

    def __del__(self):
        if hasattr(self, "connection"):
            self.engine.dispose()


class PostgresDialect(Dialect):
    def gen_all_databases_query(self):
        return "SELECT datname FROM pg_database WHERE datistemplate = false;"


class MysqlDialect(Dialect):
    def gen_all_databases_query(self):
        return "SHOW DATABASES"


class OracleDialect(Dialect):
    @property
    def username(self):
        conn_string = str(self.engine.url)
        return conn_string.split(":")[1].replace("//", "")

    def get_all_databases(self):
        return [self.username]

    def gen_all_databases_query(self):
        # You can think of a mysql "database" as a schema/user in Oracle.
        return "SELECT username FROM dba_users;"


class SqliteDialect(Dialect):
    def get_all_databases(self):
        available_dbs = self.engine.execute("PRAGMA database_list;").fetchall()
        dbs = [available_dbs[0][-1].split("/")[-1]]
        return dbs if dbs != [""] else [available_dbs[0][1]]


class MssqlDialect(Dialect):
    def gen_all_databases_query(self):
        """
        Each dbms has a different query to get user databases names.

        :return: SQL query to fetch all databases
        """
        return ("SELECT name FROM sys.databases "
                "WHERE name NOT IN ('master','tempdb','model','msdb');")


DIALECT_REGISTRY = {
    "sqlite": SqliteDialect,
    "oracle": OracleDialect,
    "mssql": MssqlDialect,
    "postgres": PostgresDialect,
    "mysql": MysqlDialect,
}


def get_dialect(sqla_engine):
    # type: (Engine) -> Dialect
    dialect_name = get_dialect_name(sqla_engine.url)
    dialect_class = DIALECT_REGISTRY.get(dialect_name)
    if not dialect_class:
        raise AttributeError("Unknown dialect: {}".format(dialect_name))

    return dialect_class(sqla_engine)


def get_dialect_name(conn_string):
    # type: (Text) -> Text
    """
    Get the dbms from the connection string.

    example:
    when connection string = 'oracle://scott:tiger@127.0.0.1:1521/sidname'
    it returns 'oracle'.
    """
    dialect = str(conn_string).split(":")[0]
    if "+" in dialect:
        dialect = dialect.split("+")[0]
    # just for postgres
    dialect = dialect.replace("postgresql", "postgres")

    return dialect
