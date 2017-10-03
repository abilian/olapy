from __future__ import absolute_import, division, print_function

from sqlalchemy import create_engine
from .olapy_config_file_parser import DbConfigParser


class MyDB(object):
    """Connect to sql database (postgres only right now)."""

    def __init__(self, db_config_file_path=None, db=None):

        # TODO temporary
        db_config = DbConfigParser(config_path=db_config_file_path)
        db_credentials = db_config.get_db_credentials()[0]
        self.sgbd = db_credentials['sgbd']
        self.username = db_credentials['user_name']
        self.password = db_credentials['password']
        self.host = db_credentials['host']
        self.port = db_credentials['port']
        self.eng, self.con_db = self._get_init_table(self.sgbd)

        if db is None:

            # todo directly in the conf file
            if self.sgbd.upper() == 'MSSQL':
                # TODO  other drivers !!!
                driver = db_credentials['driver']
                if 'LOCALHOST' in self.username.upper() or not self.username:
                    self.engine = create_engine(
                        'mssql+pyodbc://(local)/msdb?driver={0}'.format(driver.replace(' ', '+')))
                else:
                    self.engine = create_engine(
                        'mssql+pyodbc://{0}:{1}@{2}/msdb?driver={3}'.format(self.username, self.password, self.host,
                                                                            driver.replace(' ', '+')))
            #  select distinct username from dba_users where username not in ('DIP','XS$NULL','MDSYS');
            else:

                # first i want to show all databases to user (in excel)
                # self.engine = pg.connect("user={0} password={1} host='{2}'".
                #                              format(username, password, host))

                # mssql+pyodbc://(local)/msdb?driver=SQL+Server+Native+Client+11.0
                self.engine = create_engine(
                    '{0}+{1}://{2}:{3}@{4}:{5}{6}'.format(
                        self.sgbd,
                        self.eng,
                        self.username,
                        self.password,
                        self.host,
                        self.port,
                        self.con_db,),
                    encoding='utf-8',)

        #         engine = create_engine('oracle://scott:tiger@127.0.0.1:1521/sidname')

        else:

            if self.sgbd.upper() == 'MSSQL':
                # TODO  other drivers !!!

                self.engine = create_engine(
                    'mssql+pyodbc://(local)/{0}?driver=SQL+Server+Native+Client+11.0'.format(db), encoding='utf-8')
                # self.engine = create_engine('mssql+pyodbc://(local)/{0}?driver=SQL+Server+Native+Client+11.0'.format(db))
            else:
                # and then we connect to the user db
                self.engine = create_engine(
                    '{0}+{1}://{2}:{3}@{4}:{5}/{6}'.format(
                        self.sgbd,
                        self.eng,
                        self.username,
                        self.password,
                        self.host,
                        self.port,
                        db),
                    encoding='utf-8',)
                # self.connection = pg.connect(
                #     "user={0} password={1} dbname='{2}' host='{3}'".format(
                #         username, password, db, host))

    @staticmethod
    def _get_init_table(sgbd):
        if sgbd.upper() == 'POSTGRES':
            con_db = 'postgres'
            engine = 'psycopg2'
        elif sgbd.upper() == 'MYSQL':
            con_db = ''
            engine = 'mysqldb'
        elif sgbd.upper() == 'MSSQL':
            con_db = 'msdb'
            engine = 'pyodbc'
        elif sgbd.upper() == 'ORACLE':
            con_db = ''
            engine = 'cx_oracle'
        else:
            con_db = ''
            engine = ''

        return engine, con_db

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()
