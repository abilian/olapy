import psycopg2 as pg

# postgres connection
from olapy_config_file_parser import DbConfigParser

class MyDB(object):
    """Connect to sql database (postgres only right now)."""

    # @staticmethod
    # def db_credentials():
    #     db_config = DbConfigParser()
    #     if db_config.config_file_exist():
    #         # many databases in the future maybe
    #         return db_config.get_db_credentials()[0]
    #     else:
    #         raise Exception('Missing database config file')


    def __init__(self,
                 username='postgres',
                 password='root',
                 db=None,
                 host='localhost'
                 ):

        # TODO temporary
        db_config = DbConfigParser()
        db_credentials = db_config.get_db_credentials()[0]
        username = db_credentials['user_name']
        password = db_credentials['password']
        host = db_credentials['host']

        if db is None:

            self.connection = pg.connect("user={0} password={1} host='{2}'".
                                         format(username, password, host))
        else:
            try:
                self.connection = pg.connect(
                    "user={0} password={1} dbname='{2}' host='{3}'".format(
                        username, password, db, host))
            except:
                print("can't connect")

    def __del__(self):
        if hasattr(self, 'connection'):
            self.connection.close()
