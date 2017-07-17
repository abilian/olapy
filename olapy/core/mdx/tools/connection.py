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
        username = db_credentials['user_name']
        password = db_credentials['password']
        host = db_credentials['host']
        port = db_credentials['port']

        if db is None:
            # first i want to show all databases to user (in excel)
            # self.engine = pg.connect("user={0} password={1} host='{2}'".
            #                              format(username, password, host))
            self.engine = create_engine(
                'postgresql+psycopg2://{0}:{1}@{3}:{4}/{2}'.format(
                    username, password, 'postgres', host, port))

        else:
            # and then we connect to the user db
            self.engine = create_engine(
                'postgresql+psycopg2://{0}:{1}@{3}:{4}/{2}'.format(
                    username, password, db, host, port))
            # self.connection = pg.connect(
            #     "user={0} password={1} dbname='{2}' host='{3}'".format(
            #         username, password, db, host))

    def __del__(self):
        if hasattr(self, 'connection'):
            self.engine.dispose()
