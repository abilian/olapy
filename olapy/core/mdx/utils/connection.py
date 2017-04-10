import psycopg2 as pg


class MyDB(object):
    def __init__(self, username='postgres', password='root', db=None):
        if db is None:
            self.connection = pg.connect(
                "user={0} password={1}".format(username, password))
        else:
            self.connection = pg.connect("user={0} password={1} dbname='{2}'".
                                         format(username, password, db))

    def __del__(self):
        self.connection.close()
