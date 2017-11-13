"""
Managing all database access
"""
from __future__ import absolute_import, division, print_function

import os
from sqlalchemy import create_engine


# todo cleannnnnnnnnnnn

def _get_dbms_from_conn_string(conn_string):
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


def _get_init_table(dbms):
    """
    some dbms have default database so we can connect to the dbms without connecting to a specific database
    :param dbms: postgres, oracle....
    :return: default database name
    """
    if dbms.upper() == 'POSTGRES':
        con_db = '/postgres'
        engine = 'postgresql+psycopg2'
    elif dbms.upper() == 'MYSQL':
        con_db = ''
        engine = 'mysql+mysqldb'
    elif dbms.upper() == 'MSSQL':
        con_db = 'msdb'
        engine = 'mssql+pyodbc'
    elif dbms.upper() == 'ORACLE':
        con_db = ''
        engine = 'oracle+cx_oracle'
    else:
        con_db = ''
        engine = ''

    return engine, con_db


def _connect_to_mssql(db_credentials, driver='mssql+pyodbc', db=None):
    """
    As always, microsoft ruin our life, to access sql server you need to add driver clause to the connection string, we do this here.

    :param db_credentials: olapy database config parser obj
    :param driver: driver to user for sql server, by default mssql+pyodbc
    :param db: database to connect to
    :return: SqlAlchemy engine
    """
    # todo recheck + clean
    sql_server_driver = db_credentials['sql_server_driver'].replace(' ', '+')
    if db is not None:
        url = driver + '://(local)/{0}?driver={1}'.format(db, sql_server_driver)
        return create_engine(url, encoding='utf-8')

    if 'LOCALHOST' in db_credentials['user'].upper() or not db_credentials['user']:
        url = driver + '://(local)/msdb?driver={0}'.format(sql_server_driver)
    else:
        url = driver + '://{0}:{1}@{2}:{3}/msdb?driver={4}'.format(db_credentials['user'],
                                                                   db_credentials['password'],
                                                                   db_credentials['host'], db_credentials['port'],
                                                                   sql_server_driver)
    return create_engine(url)


def _construct_engine(db, db_credentials):
    """
    Create the SqlAlchemy object which will use it to connect to database.

    :param db: database to connect to
    :param db_credentials: olapy database config parser obj
    :return: SqlAlchemy engine
    """
    eng, con_db = _get_init_table(db_credentials['dbms'])
    if db is None:
        if db_credentials['dbms'].upper() == 'MSSQL':
            return _connect_to_mssql(db_credentials)
        else:
            # Show all databases to user (in excel)
            url = '{0}://{1}:{2}@{3}:{4}{5}'.format(eng, db_credentials['user'], db_credentials['password'],
                                                    db_credentials['host'], db_credentials['port'], con_db)
            return create_engine(url, encoding='utf-8')

    else:
        if db_credentials['dbms'].upper() == 'MSSQL':
            return _connect_to_mssql(db=db, db_credentials=db_credentials)
        else:
            # and then we connect to the user db
            url = '{0}://{1}:{2}@{3}:{4}/{5}'.format(eng, db_credentials['user'], db_credentials['password'],
                                                     db_credentials['host'], db_credentials['port'],
                                                     '' if db_credentials['dbms'].upper() == 'ORACLE' else db)
            return create_engine(url, encoding='utf-8')


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
            conn_string = os.environ["SQLALCHEMY_DATABASE_URI"]
            if db is not None:
                # todo test this with windows
                conn_string = (conn_string + '/' + db)
            self.engine = create_engine(conn_string)
            self.dbms = _get_dbms_from_conn_string(conn_string)
            # oracle://scott:tiger@127.0.0.1:1521/sidname
            self.username = conn_string.split(':')[1].replace('//', '')

        else:
            db_credentials = db_config.get_db_credentials()
            self.dbms = db_credentials['dbms']
            self.username = db_credentials['user']
            self.engine = _construct_engine(db, db_credentials)

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()
