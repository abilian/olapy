from collections import defaultdict
from sqlalchemy import create_engine
import yaml

try:
    config = yaml.load(open('db_config', 'r'))
except FileExistsError:
    raise FileExistsError()

DB_CONFIG_DEFAULTS = {
    # for sql server 'driver':mssql+pyodbc or mssql+pymssql
    'driver': config['driver'],
    'host': config['host'],
    'port': config['port'],
    'name': config['db_name'],
    'user': config['user'],
    'pass': config['password'],
}

DSN_TEMPLATE = '{driver}://{user}:{pass}@{host}:{port}/{name}'


def create_db_engine(driver='SQL Server Native Client', version='11.0'):
    config = defaultdict(**DB_CONFIG_DEFAULTS)
    dsn = DSN_TEMPLATE.format(**config)

    if DB_CONFIG_DEFAULTS['driver'].upper() == 'POSTGRES':
        dsn += '?client_encoding=utf8'
    elif 'MSSQL' in DB_CONFIG_DEFAULTS['driver'].upper():
        dsn += '?driver={0}'.format(driver + ' ' + version)
    return create_engine(dsn)


def get_services():
    return {
        'sqlalchemy.engine': create_db_engine()
    }
