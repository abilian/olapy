"""
Bonobo etl require this file to access databases (and files)
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
from collections import defaultdict

from sqlalchemy import create_engine

from olapy.core.mdx.tools.olapy_config_file_parser import DbConfigParser

db_config = DbConfigParser()
config = db_config.get_db_credentials()

DB_CONFIG_DEFAULTS = {
    'driver': config['driver'],
    'host': config['host'],
    'port': config['port'],
    'name': config['db_name'],
    'user': config['user'],
    'pass': config['password'],
}

DSN_TEMPLATE = '{driver}://{user}:{pass}@{host}:{port}/{name}'


def create_db_engine(driver='SQL Server Native Client', version='11.0'):
    if 'SQLALCHEMY_DATABASE_URI' in os.environ:
        dsn = os.environ['SQLALCHEMY_DATABASE_URI']
    else:
        config = defaultdict(**DB_CONFIG_DEFAULTS)
        dsn = DSN_TEMPLATE.format(**config)

        if DB_CONFIG_DEFAULTS['driver'].upper() == 'POSTGRES':
            dsn += '?client_encoding=utf8'
        elif 'MSSQL' in DB_CONFIG_DEFAULTS['driver'].upper():
            dsn += '?driver={}'.format(driver + ' ' + version)
    return create_engine(dsn)


def get_services():
    return {
        'sqlalchemy.engine': create_db_engine(),
    }
