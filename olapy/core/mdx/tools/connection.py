"""
Managing all database access
"""
from __future__ import absolute_import, division, print_function

import os

from sqlalchemy import create_engine


class MyDB(object):
    """Connect to sql database."""

    def __init__(self, db_config, db_name=None):
        """
        Connection can be made either with connection string provided from
        environment variable 'SQLALCHEMY_DATABASE_URI', or with olapy config
        file parser obj.

        :param db_config: olapy config file obj
        :param db_name: database name to connect to
        """
        if 'SQLALCHEMY_DATABASE_URI' in os.environ:
            self.conn_string = os.environ["SQLALCHEMY_DATABASE_URI"]
            self.engine, self.dbms = self.connect_with_env_var(None)
            # self.engine, self.dbms = self.connect_with_env_var(db_name)
        else:
            self.db_credentials = db_config
            self.engine, self.dbms = self.connect_without_env_var(db_name)

    def gen_all_databases_query(self):
        """
        Each dbms has different query to get user databases names.

        :return: sql query to fetch all databases
        """
        if self.dbms.upper() == 'POSTGRES':
            return 'SELECT datname FROM pg_database WHERE datistemplate = false;'
        elif self.dbms.upper() == 'MYSQL':
            return 'SHOW DATABASES'

    @staticmethod
    def get_dbms_from_conn_string(conn_string):
        """
        Get the dbms from the connection string.

        example:
        connection string => oracle://scott:tiger@127.0.0.1:1521/sidname
        it returns oracle

        :param conn_string: connection string
        :return: dbms
        """
        db = conn_string.split(':')[0]
        if '+' in db:
            db = db.split('+')[0]
        # just for postgres
        db = db.replace('postgresql', 'postgres')
        return db

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

    def connect_with_env_var(self, db):
        if db is not None:
            self.conn_string = self.conn_string.rstrip('/')
            engine = create_engine(self.conn_string + '/' + db)
        else:
            engine = create_engine(self.conn_string)

        dbms = MyDB.get_dbms_from_conn_string(self.conn_string)

        return engine, dbms

    def connect_without_env_var(self, db):
        dbms = self.db_credentials['dbms']
        engine = self.construct_engine(db)
        return engine, dbms

    def get_init_table(self):
        """
        Some dbms have default database so we can connect to the dbms
        without connecting to a specific database.

        :return: default database name
        """
        if self.db_credentials['dbms'].upper() == 'POSTGRES':
            con_db = 'postgres'
            engine = 'postgresql+psycopg2'
        elif self.db_credentials['dbms'].upper() == 'MYSQL':
            con_db = ''
            engine = 'mysql+mysqldb'
        else:
            con_db = ''
            engine = ''
        return engine, con_db

    def construct_engine(self, db):
        """
        Create the SqlAlchemy engine object which will use it to connect to database.

        :param db: database to connect to
        :return: SqlAlchemy engine
        """
        eng, con_db = self.get_init_table()
        if db is None:
            db_to_connect_to = con_db
        else:
            db_to_connect_to = '' if self.db_credentials[
                'dbms'].upper() == 'ORACLE' else db

        url = '{}://{}:{}@{}:{}/{}'.format(
            eng,
            self.db_credentials['user'],
            self.db_credentials['password'],
            self.db_credentials['host'],
            self.db_credentials['port'],
            db_to_connect_to,
        )

        return create_engine(url, encoding='utf-8')

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()


class MyOracleDB(MyDB):

    def __init__(self, db_config, db_name=None):
        MyDB.__init__(self, db_config, db_name=db_name)

    @property
    def username(self):
        if 'SQLALCHEMY_DATABASE_URI' in os.environ:
            conn_string = os.environ["SQLALCHEMY_DATABASE_URI"]
            return conn_string.split(':')[1].replace('//', '')
        else:
            return self.db_credentials['user']

    def get_all_databases(self):
        return [self.username]

    def gen_all_databases_query(self):
        # You can think of a mysql "database" as a schema/user in Oracle.
        return 'SELECT username FROM dba_users;'

    def get_init_table(self):
        con_db = ''
        engine = 'oracle+cx_oracle'
        return engine, con_db


class MySqliteDB(MyDB):

    def __init__(self, db_config, db_name=None):
        MyDB.__init__(self, db_config, db_name=db_name)

    def construct_engine(self, db=None):
        return create_engine('sqlite:///' + self.db_credentials['path'])

    def get_all_databases(self):
        available_dbs = self.engine.execute('PRAGMA database_list;').fetchall()
        dbs = [available_dbs[0][-1].split('/')[-1]]
        return dbs if dbs != [''] else [available_dbs[0][1]]

    def connect_with_env_var(self, db):
        if self.conn_string.split(':/')[0].upper() == 'SQLITE':
            engine = create_engine(self.conn_string)
            dbms = 'SQLITE'
            return engine, dbms

    def connect_without_env_var(self, db):
        dbms = 'SQLITE'
        engine = self.construct_engine()
        return engine, dbms


class MyMssqlDB(MyDB):

    def __init__(self, db_config, db_name=None):
        MyDB.__init__(self, db_config, db_name=db_name)

    def get_init_table(self):
        con_db = 'msdb'
        engine = 'mssql+pyodbc'
        return engine, con_db

    def gen_all_databases_query(self):
        """
        Each dbms has a different query to get user databases names.

        :return: SQL query to fetch all databases
        """
        return "SELECT name FROM sys.databases WHERE name NOT IN ('master','tempdb','model','msdb');"

    def _connect_to_mssql(self, driver='mssql+pyodbc', db=None):
        """
        As always, Microsoft ruins our life, to access SQL Server you need
        to add a driver clause to the connection string; we do this here.

        :param driver: driver to user for sql server, by default mssql+pyodbc
        :param db: database to connect to
        :return: SqlAlchemy engine
        """
        sql_server_driver = self.db_credentials['sql_server_driver'].replace(' ', '+')
        if db is not None:
            url = driver + '://(local)/{}?driver={}'.format(db, sql_server_driver)
            return create_engine(url, encoding='utf-8')

        if 'LOCALHOST' in self.db_credentials['user'].upper() or not self.db_credentials['user']:
            url = driver + '://(local)/msdb?driver={}'.format(sql_server_driver)
        else:
            url = driver + '://{}:{}@{}:{}/msdb?driver={}'.format(
                self.db_credentials['user'],
                self.db_credentials['password'],
                self.db_credentials['host'],
                self.db_credentials['port'],
                sql_server_driver,
            )
        return create_engine(url)

    def construct_engine(self, db):
        return self._connect_to_mssql(db=db)
