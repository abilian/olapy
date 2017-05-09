import psycopg2 as pg

# postgres connection
USERNAME = 'postgres'
PASSWORD = 'root'
HOST = 'localhost'

class MyDB(object):
    """Connect to sql database (postgres only right now)."""
    def __init__(self,
                 username=USERNAME,
                 password=PASSWORD,
                 db=None,
                 host=HOST):
        if db is None:
            self.connection = pg.connect(
                "user={0} password={1}".format(username, password))
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
