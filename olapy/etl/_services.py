from collections import defaultdict
from sqlalchemy import create_engine

# *****mysql default config****
# DRIVER = 'mysql'
# HOST = 'localhost'
# PORT = '3306'
# DB_NAME = 'sales_mysql'
# USER = 'root'
# PASSWORD = 'toor'

# *****postgres default config****
DRIVER = 'postgres'
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'tutorial'
USER = 'tutorial'
PASSWORD = 'tutorial'

DB_CONFIG_DEFAULTS = {
    'driver': DRIVER,
    'host': HOST,
    'port': PORT,
    'name': DB_NAME,
    'user': USER,
    'pass': PASSWORD,
}

DSN_TEMPLATE = '{driver}://{user}:{pass}@{host}:{port}/{name}'


def create_db_engine():
    config = defaultdict(**DB_CONFIG_DEFAULTS)
    dsn = DSN_TEMPLATE.format(**config)

    if DB_CONFIG_DEFAULTS['driver'].upper() == 'POSTGRES':
        dsn += '?client_encoding=utf8'
    return create_engine(dsn)


def get_services():
    return {
        'sqlalchemy.engine': create_db_engine()
    }
