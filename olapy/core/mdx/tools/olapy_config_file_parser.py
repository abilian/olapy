"""
OlaPy config file parser object contains credentials that allows olapy to access database \
(of course this happens only if SQLALCHEMY_DATABASE_URI Environment variable is not specified
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os

import yaml


class DbConfigParser:
    """
    Class to construct to olapy database config object
    """

    def __init__(self, config_file_path=None):
        """

        :param config_file_path:  config file path (DEFAULT = cubes-config)
        """

        if config_file_path is None:
            from os.path import expanduser
            home_directory = expanduser("~")
            self.config_file_path = os.path.join(
                home_directory,
                'olapy-data',
                'olapy-config.yml',
            )
        else:
            self.config_file_path = config_file_path

    def get_db_credentials(self, db_config_path=None):
        """
        Get all db credentials in the config file.

        :return: dict of database connection credentials.
        """
        if db_config_path:
            file_path = db_config_path
        else:
            file_path = self.config_file_path

        with open(file_path) as config_file:
            # try:
            config = yaml.load(config_file)
            dbms = config['dbms']

            if dbms.upper() == 'SQLITE':
                path = config['path'] if 'path' in config.keys() and dbms.upper(
                ) == 'SQLITE' else None
                credentials = {
                    'dbms': dbms,
                    'path': path,
                }

            else:
                user = config['user'] if 'user' in config.keys() else ''
                password = config['password'] if 'LOCALHOST' not in config[
                    'user'].upper() else ''
                host = config['host'] if 'LOCALHOST' not in config[
                    'user'].upper() else ''
                port = config['port'] if 'port' in config.keys() else ''
                sql_server_driver = config[
                    'sql_server_driver'] if 'sql_server_driver' in config.keys(
                ) else ''
                db_name = config[
                    'db_name'] if 'db_name' in config.keys() else ''
                driver = config['driver'] if 'driver' in config.keys() else ''

                credentials = {
                    'dbms': dbms,
                    'user': user,
                    'password': password,
                    'host': host,
                    'port': port,
                    'db_name': db_name,
                    'driver': driver,
                    'sql_server_driver': sql_server_driver,
                }

        return credentials
