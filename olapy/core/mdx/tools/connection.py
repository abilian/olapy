from __future__ import absolute_import, division, print_function

# import psycopg2 as pg
from sqlalchemy import create_engine

# postgres connection
from .olapy_config_file_parser import DbConfigParser


class MyDB(object):
    """Connect to sql database (postgres only right now)."""

    def __init__(self, db_config_file_path=None, db=None):

        # TODO temporary
        db_config = DbConfigParser(config_path=db_config_file_path)
        db_credentials = db_config.get_db_credentials()[0]
        self.sgbd = db_credentials['sgbd']
        username = db_credentials['user_name']
        password = db_credentials['password']
        host = db_credentials['host']
        port = db_credentials['port']
        # mysql+mysqldb://scott:tiger@localhost/foo
        eng, con_db = self._get_init_table(self.sgbd)

        if db is None:
            # first i want to show all databases to user (in excel)
            # self.engine = pg.connect("user={0} password={1} host='{2}'".
            #                              format(username, password, host))
            self.engine = create_engine(
                '{0}+{1}://{2}:{3}@{4}:{5}{6}'.format(
                    self.sgbd,
                    eng,
                    username,
                    password,
                    host,
                    port,
                    con_db,),
                encoding='utf-8',)

        else:
            # and then we connect to the user db
            self.engine = create_engine(
                '{0}+{1}://{2}:{3}@{4}:{5}/{6}'.format(
                    self.sgbd,
                    eng,
                    username,
                    password,
                    host,
                    port,
                    db),
                encoding='utf-8',)
            # self.connection = pg.connect(
            #     "user={0} password={1} dbname='{2}' host='{3}'".format(
            #         username, password, db, host))

    @staticmethod
    def _get_init_table(sgbd):
        if sgbd.upper() == 'POSTGRES':
            con_db = '/postgres'
            engine = 'psycopg2'
        elif sgbd.upper() == 'MYSQL':
            con_db = ''
            engine = 'mysqldb'
        else:
            con_db = ''
            engine = ''

        return engine, con_db

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()
