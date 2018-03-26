"""
Managing all database access
"""
from __future__ import absolute_import, division, print_function


def get_dialect(sqla_engine):
    dialect_name = get_dialect_name(sqla_engine.url)
    if 'sqlite' in dialect_name:
        dialect = SqliteDialect(sqla_engine)
    elif 'oracle' in dialect_name:
        dialect = OracleDialect(sqla_engine)
    elif 'mssql' in dialect_name:
        dialect = MssqlDialect(sqla_engine)
    elif 'postgres' in dialect_name:
        dialect = PostgresDialect(sqla_engine)
    elif 'mysql' in dialect_name:
        dialect = MysqlDialect(sqla_engine)
    else:
        raise AttributeError("Unknown dialect: {}".format(dialect_name))
    return dialect


def get_dialect_name(conn_string):
    """
    Get the dbms from the connection string.

    example:
    when connection string = 'oracle://scott:tiger@127.0.0.1:1521/sidname'
    it returns 'oracle'

    :param conn_string: connection string
    :return: dbms
    """
    dialect = str(conn_string).split(':')[0]
    if '+' in dialect:
        dialect = dialect.split('+')[0]
    # just for postgres
    dialect = dialect.replace('postgresql', 'postgres')

    return dialect


class BaseDialect(object):
    """Connect to sql database."""

    def __init__(self, sqla_engine=None):
        """
        Connection can be made either with connection string provided from
        environment variable 'SQLALCHEMY_DATABASE_URI', or with olapy config
        file parser obj.

        :param db_name: database name to connect to
        """

        self.engine = sqla_engine

    def gen_all_databases_query(self):
        """
        Each dbms has different query to get user databases names.

        :return: sql query to fetch all databases
        """
        raise NotImplementedError

    def get_all_databases(self):
        all_db_query = self.gen_all_databases_query()
        result = self.engine.execute(all_db_query)
        available_tables = result.fetchall()
        return [
            database[0]
            for database in available_tables
            if database[0] not in
               ['mysql', 'information_schema', 'performance_schema', 'sys']
        ]

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()


class PostgresDialect(BaseDialect):
    def gen_all_databases_query(self):
        return 'SELECT datname FROM pg_database WHERE datistemplate = false;'


class MysqlDialect(BaseDialect):
    def gen_all_databases_query(self):
        return 'SHOW DATABASES'


class OracleDialect(BaseDialect):

    @property
    def username(self):
        conn_string = str(self.engine.url)
        return conn_string.split(':')[1].replace('//', '')

    def get_all_databases(self):
        return [self.username]

    def gen_all_databases_query(self):
        # You can think of a mysql "database" as a schema/user in Oracle.
        return 'SELECT username FROM dba_users;'


class SqliteDialect(BaseDialect):

    def get_all_databases(self):
        available_dbs = self.engine.execute('PRAGMA database_list;').fetchall()
        dbs = [available_dbs[0][-1].split('/')[-1]]
        return dbs if dbs != [''] else [available_dbs[0][1]]


class MssqlDialect(BaseDialect):

    # def get_init_table(self):
    #     con_db = 'msdb'
    #     engine = 'mssql+pyodbc'
    #     return engine, con_db

    def gen_all_databases_query(self):
        """
        Each dbms has a different query to get user databases names.

        :return: SQL query to fetch all databases
        """
        return "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb');"
