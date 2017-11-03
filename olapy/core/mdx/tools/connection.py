from __future__ import absolute_import, division, print_function

from sqlalchemy import create_engine
from .olapy_config_file_parser import DbConfigParser


class MyDB(object):
    """Connect to sql database (postgres only right now)."""

    def __init__(self, db_config_file_path=None, db=None):
        db_config = DbConfigParser(config_path=db_config_file_path)
        db_credentials = db_config.get_db_credentials()
        self.sgbd = db_credentials['sgbd']
        self.username = db_credentials['user_name']
        self.password = db_credentials['password']
        self.host = db_credentials['host']
        self.port = db_credentials['port']
        self.eng, self.con_db = self._get_init_table(self.sgbd)

        if db is None:
            if self.sgbd.upper() == 'MSSQL':
                self.engine = self._connect_to_mssql(db_credentials['sql_server_driver'].replace(' ', '+'))
            else:
                # Show all databases to user (in excel)
                self.engine = create_engine(
                    '{0}://{1}:{2}@{3}:{4}{5}'.format(self.eng, self.username, self.password, self.host, self.port,
                                                      self.con_db),
                    encoding='utf-8'
                )

        else:
            if self.sgbd.upper() == 'MSSQL':
                self.engine = self._connect_to_mssql(db=db,
                                                     sql_server_driver=db_credentials['sql_server_driver'].replace(' ',
                                                                                                                   '+'))
            else:
                # and then we connect to the user db
                self.engine = create_engine(
                    '{0}://{1}:{2}@{3}:{4}/{5}'.format(
                        self.eng, self.username, self.password, self.host,
                        self.port,
                        '' if self.sgbd.upper() == 'ORACLE' else db),
                    encoding='utf-8'
                )

    def _connect_to_mssql(self, sql_server_driver, driver='mssql+pyodbc', db=None):
        # todo recheck + clean
        if db is not None:
            return create_engine(driver + '://(local)/{0}?driver={1}'.format(db, sql_server_driver),
                                 encoding='utf-8'
                                 )

        if 'LOCALHOST' in self.username.upper() or not self.username:
            return create_engine(
                driver + '://(local)/msdb?driver={0}'.format(sql_server_driver))
        else:
            return create_engine(
                driver + '://{0}:{1}@{2}:{3}/msdb?driver={4}'.format(self.username,
                                                                     self.password,
                                                                     self.host,
                                                                     self.port,
                                                                     sql_server_driver))

    @staticmethod
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

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()
