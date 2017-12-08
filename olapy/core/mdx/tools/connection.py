"""
Managing all database access
"""
from __future__ import absolute_import, division, print_function

import os
from sqlalchemy import create_engine


# todo cleannnnnnnnnnnn
#
# def _get_dbms_from_conn_string(conn_string):
#     """
#     get the dbms from the connection string
#     example:
#     connection string => oracle://scott:tiger@127.0.0.1:1521/sidname
#     it returns oracle
#     :param conn_string: connection string
#     :return: dbms
#     """
#     db = conn_string.split(':')[0]
#     if '+' in db:
#         db = db.split('+')[0]
#     # just for postgres
#     db = db.replace('postgresql', 'postgres')
#     return db

# def _get_init_table(dbms):
#     """
#     some dbms have default database so we can connect to the dbms without connecting to a specific database
#     :param dbms: postgres, oracle....
#     :return: default database name
#     """
#     if dbms.upper() == 'POSTGRES':
#         con_db = '/postgres'
#         engine = 'postgresql+psycopg2'
#     elif dbms.upper() == 'MYSQL':
#         con_db = ''
#         engine = 'mysql+mysqldb'
#     elif dbms.upper() == 'MSSQL':
#         con_db = 'msdb'
#         engine = 'mssql+pyodbc'
#     elif dbms.upper() == 'ORACLE':
#         con_db = ''
#         engine = 'oracle+cx_oracle'
#     else:
#         con_db = ''
#         engine = ''

# return engine, con_db


# def _connect_to_sqlite(db_credentials):
#     """
#     As always, microsoft ruin our life, to access sql server you need to add driver clause to the connection string, we do this here.
#
#     :param db_credentials: olapy database config parser obj
#     :param driver: driver to user for sql server, by default mssql+pyodbc
#     :param db: database to connect to
#     :return: SqlAlchemy engine
#     """
#     return create_engine('sqlite:///' + db_credentials['path'])

#
# def _connect_to_mssql(db_credentials, driver='mssql+pyodbc', db=None):
#     """
#     As always, microsoft ruin our life, to access sql server you need to add driver clause to the connection string, we do this here.
#
#     :param db_credentials: olapy database config parser obj
#     :param driver: driver to user for sql server, by default mssql+pyodbc
#     :param db: database to connect to
#     :return: SqlAlchemy engine
#     """
#     # todo recheck + clean
#     sql_server_driver = db_credentials['sql_server_driver'].replace(' ', '+')
#     if db is not None:
#         url = driver + '://(local)/{0}?driver={1}'.format(db, sql_server_driver)
#         return create_engine(url, encoding='utf-8')
#
#     if 'LOCALHOST' in db_credentials['user'].upper() or not db_credentials['user']:
#         url = driver + '://(local)/msdb?driver={0}'.format(sql_server_driver)
#     else:
#         url = driver + '://{0}:{1}@{2}:{3}/msdb?driver={4}'.format(db_credentials['user'],
#                                                                    db_credentials['password'],
#                                                                    db_credentials['host'], db_credentials['port'],
#                                                                    sql_server_driver)
#     return create_engine(url)
#
#
# def _construct_engine(db, db_credentials):
#     """
#     Create the SqlAlchemy object which will use it to connect to database.
#
#     :param db: database to connect to
#     :param db_credentials: olapy database config parser obj
#     :return: SqlAlchemy engine
#     """
#     eng, con_db = _get_init_table(db_credentials['dbms'])
#     if db is None:
#         if db_credentials['dbms'].upper() == 'MSSQL':
#             return _connect_to_mssql(db_credentials)
#         elif db_credentials['dbms'].upper() == 'SQLITE':
#             return _connect_to_sqlite(db_credentials)
#         else:
#             # Show all databases to user (in excel)
#             url = '{0}://{1}:{2}@{3}:{4}{5}'.format(eng, db_credentials['user'], db_credentials['password'],
#                                                     db_credentials['host'], db_credentials['port'], con_db)
#             return create_engine(url, encoding='utf-8')
#
#     else:
#         if db_credentials['dbms'].upper() == 'MSSQL':
#             return _connect_to_mssql(db=db, db_credentials=db_credentials)
#         elif db_credentials['dbms'].upper() == 'SQLITE':
#             return _connect_to_sqlite(db_credentials=db_credentials)
#         else:
#             # and then we connect to the user db
#             url = '{0}://{1}:{2}@{3}:{4}/{5}'.format(eng, db_credentials['user'], db_credentials['password'],
#                                                      db_credentials['host'], db_credentials['port'],
#                                                      '' if db_credentials['dbms'].upper() == 'ORACLE' else db)
#             return create_engine(url, encoding='utf-8')


class MyDB(object):
    """Connect to sql database."""

    def __init__(self, db_config, db=None):
        """
        Connection can be made either with connection string provided from \
        environment variable 'SQLALCHEMY_DATABASE_URI', or with olapy config file parser obj
        :param db_config: olapy config file obj
        :param db: database name to connect to
        """
        if 'SQLALCHEMY_DATABASE_URI' in os.environ.keys():
            print('1111111111111111111')
            self.conn_string = os.environ["SQLALCHEMY_DATABASE_URI"]
            self.engine, self.dbms = self.connect_with_env_var(db)

        else:
            print('222222222222222222222')
            self.db_credentials = db_config.get_db_credentials()
            self.engine, self.dbms = self.connect_without_env_var(db)
            print(self.dbms)
            # self.engine, self.dbms, self.username, self.path = self.connect_without_env_var(db, self.db_credentials)

    @staticmethod
    def get_dbms_from_conn_string(conn_string):
        """
        get the dbms from the connection string
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

    def connect_with_env_var(self, db):

        # if conn_string.split(':/')[0].upper() == 'SQLITE':
        #     engine = create_engine(conn_string)
        #     dbms = 'SQLITE'
        #     return engine, dbms

        if db is not None:
            # todo test this with windows
            engine = create_engine(self.conn_string + '/' + db)
        else:
            engine = create_engine(self.conn_string)

        dbms = MyDB.get_dbms_from_conn_string(self.conn_string)
        # oracle://scott:tiger@127.0.0.1:1521/sidname
        # username = conn_string.split(':')[1].replace('//', '')

        return engine, dbms

    def connect_without_env_var(self, db):
        # username, path = None, None
        dbms = self.db_credentials['dbms']
        # if dbms.upper() == 'SQLITE':
        #     path = db_credentials['path']
        # else:
        #     username = db_credentials['user']
        print('0000000000000')
        print(dbms)
        engine = self.construct_engine(db)

        return engine, dbms
        # return engine, dbms, username, path

    def get_init_table(self):
        """
        some dbms have default database so we can connect to the dbms without connecting to a specific database
        :param dbms: postgres, oracle....
        :return: default database name
        """
        if self.db_credentials['dbms'].upper() == 'POSTGRES':
            con_db = '/postgres'
            engine = 'postgresql+psycopg2'
        elif self.db_credentials['dbms'].upper() == 'MYSQL':
            con_db = ''
            engine = 'mysql+mysqldb'
        elif self.db_credentials['dbms'].upper() == 'MSSQL':
            con_db = 'msdb'
            engine = 'mssql+pyodbc'
        # elif self.dbms.upper() == 'ORACLE':
        #     con_db = ''
        #     engine = 'oracle+cx_oracle'
        else:
            con_db = ''
            engine = ''
        return engine, con_db

    def construct_engine(self, db):
        """
        Create the SqlAlchemy object which will use it to connect to database.

        :param db: database to connect to
        :param db_credentials: olapy database config parser obj
        :return: SqlAlchemy engine
        """
        eng, con_db = self.get_init_table()
        if db is None:
            # if db_credentials['dbms'].upper() == 'MSSQL':
            #     return _connect_to_mssql(db_credentials)
            # elif db_credentials['dbms'].upper() == 'SQLITE':
            #     return _connect_to_sqlite(db_credentials)
            # else:
            # Show all databases to user (in excel)
            url = '{0}://{1}:{2}@{3}:{4}{5}'.format(eng,
                                                    self.db_credentials['user'],
                                                    self.db_credentials['password'],
                                                    self.db_credentials['host'],
                                                    self.db_credentials['port'],
                                                    con_db)
            return create_engine(url, encoding='utf-8')

        else:
            # if db_credentials['dbms'].upper() == 'MSSQL':
            #     return _connect_to_mssql(db=db, db_credentials=db_credentials)
            # elif db_credentials['dbms'].upper() == 'SQLITE':
            #     return _connect_to_sqlite(db_credentials=db_credentials)
            # else:
            # and then we connect to the user db
            db_to_connect_to = '' if self.db_credentials['dbms'].upper() == 'ORACLE' else db
            url = '{0}://{1}:{2}@{3}:{4}/{5}'.format(eng,
                                                     self.db_credentials['user'],
                                                     self.db_credentials['password'],
                                                     self.db_credentials['host'],
                                                     self.db_credentials['port'],
                                                     db_to_connect_to)

            return create_engine(url, encoding='utf-8')

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()


class MyOracleDB(MyDB):
    def __init__(self, db_config, db=None):
        MyDB.__init__(self, db_config, db=db)

    def get_username(self):
        if 'SQLALCHEMY_DATABASE_URI' in os.environ:
            conn_string = os.environ["SQLALCHEMY_DATABASE_URI"]
            return conn_string.split(':')[1].replace('//', '')
        else:
            return self.db_credentials['user']

    def get_init_table(self):
        """
        some dbms have default database so we can connect to the dbms without connecting to a specific database
        :param dbms: postgres, oracle....
        :return: default database name
        """
        con_db = ''
        engine = 'oracle+cx_oracle'
        return engine, con_db


class MySqliteDB(MyDB):
    def __init__(self, db_config, db=None):
        MyDB.__init__(self, db_config, db=db)

    def construct_engine(self, db=None):
        """
        Create the SqlAlchemy object which will use it to connect to database.

        :param db: database to connect to
        :param db_credentials: olapy database config parser obj
        :return: SqlAlchemy engine
        """
        # eng, con_db = self.get_init_table()
        return create_engine('sqlite:///' + self.db_credentials['path'])

    def connect_with_env_var(self, db):
        if self.conn_string.split(':/')[0].upper() == 'SQLITE':
            engine = create_engine(self.conn_string)
            dbms = 'SQLITE'
            return engine, dbms

    def connect_without_env_var(self, db):
        dbms = 'SQLITE'
        engine = self.construct_engine()
        return engine, dbms
        # return engine, dbms, username, path
