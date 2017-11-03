from __future__ import absolute_import, division, print_function

import os
from sqlalchemy import create_engine
from .olapy_config_file_parser import DbConfigParser


# todo cleannnnnnnnnnnn

def _get_dbms_from_conn_string(conn_string):
    con_s = conn_string.split(':')
    if '+' in con_s[0]:
        return con_s[0].split('+')
    else:
        return con_s[0]


def _get_init_table(sgbd):
    if sgbd.upper() == 'POSTGRES':
        con_db = '/postgres'
        engine = 'postgresql+psycopg2'
    elif sgbd.upper() == 'MYSQL':
        con_db = ''
        engine = 'mysql+mysqldb'
    elif sgbd.upper() == 'MSSQL':
        con_db = 'msdb'
        engine = 'mssql+pyodbc'
    elif sgbd.upper() == 'ORACLE':
        con_db = ''
        engine = 'oracle+cx_oracle'
    else:
        con_db = ''
        engine = ''

    return engine, con_db


def _connect_to_mssql(db_credentials, driver='mssql+pyodbc', db=None):
    # todo recheck + clean
    sql_server_driver = db_credentials['sql_server_driver'].replace(' ', '+')
    if db is not None:
        return create_engine(driver + '://(local)/{0}?driver={1}'.format(db, sql_server_driver),
                             encoding='utf-8'
                             )

    if 'LOCALHOST' in db_credentials['user_name'].upper() or not db_credentials['user_name']:
        return create_engine(
            driver + '://(local)/msdb?driver={0}'.format(sql_server_driver))
    else:
        return create_engine(
            driver + '://{0}:{1}@{2}:{3}/msdb?driver={4}'.format(db_credentials['user_name'],
                                                                 db_credentials['password'],
                                                                 db_credentials['host'],
                                                                 db_credentials['port'],
                                                                 sql_server_driver))


def _construct_engine(db, db_credentials):
    eng, con_db = _get_init_table(db_credentials['sgbd'])
    if db is None:
        if db_credentials['sgbd'].upper() == 'MSSQL':
            return _connect_to_mssql(db_credentials.replace(' ', '+'))
        else:
            # Show all databases to user (in excel)
            return create_engine(
                str('{0}://{1}:{2}@{3}:{4}{5}'.format(eng, db_credentials['user_name'], db_credentials['password'],
                                                      db_credentials['host'],
                                                      db_credentials['port'],
                                                      con_db)),
                encoding='utf-8'
            )

    else:
        if db_credentials['sgbd'].upper() == 'MSSQL':
            return _connect_to_mssql(db=db, db_credentials=db_credentials)
        else:
            # and then we connect to the user db
            return create_engine(
                '{0}://{1}:{2}@{3}:{4}/{5}'.format(
                    eng, db_credentials['user_name'], db_credentials['password'], db_credentials['host'],
                    db_credentials['port'],
                    '' if db_credentials['sgbd'].upper() == 'ORACLE' else db),
                encoding='utf-8'
            )


class MyDB(object):
    """Connect to sql database (postgres only right now)."""

    def __init__(self, db_config_file_path=None, db=None):

        if 'SQLALCHEMY_DATABASE_URI' in os.environ.keys():
            conn_string = os.environ["SQLALCHEMY_DATABASE_URI"]
            if db is not None:
                # todo test this with windows
                conn_string = (conn_string + '/' + db)
            self.engine = create_engine(conn_string)
            self.sgbd = _get_dbms_from_conn_string(conn_string)

        else:
            db_config = DbConfigParser(config_path=db_config_file_path)
            db_credentials = db_config.get_db_credentials()
            self.sgbd = db_credentials['sgbd']
            self.engine = _construct_engine(db, db_credentials)

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()
