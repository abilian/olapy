"""
Olapy config file parser object contains credentials that allows olapy to access database \
(of course this happens only if SQLALCHEMY_DATABASE_URI Environment variable is not specified
"""
from __future__ import absolute_import, division, print_function, unicode_literals

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
            self.config_file_path = os.path.join(home_directory, 'olapy-data', 'olapy-config')
        else:
            self.config_file_path = config_file_path

    def config_file_exist(self):
        """
        Check whether the config file exists or not.

        :return: True | False
        """
        return os.path.isfile(self.config_file_path)

    def get_db_credentials(self):
        """
        Get all db credentials in the config file.

        :return: list of cube name as key and cube source as value (csv or postgres) (right now only postgres is supported)
        """
        with open(self.config_file_path) as config_file:
            # try:
            config = yaml.load(config_file)
            return {
                'dbms': config['dbms'],
                'user': config['user'],
                'password': config['password'] if 'LOCALHOST' not in config['user'].upper() else '',
                'host': config['host'] if 'LOCALHOST' not in config['user'].upper() else '',
                'port': config['port'],
                'db_name': config['db_name'] if 'db_name' in config.keys() else '',
                'driver': config['driver'] if 'driver' in config.keys() else ''
            }
            #
            # except OSError:
            #     print('olapy db_config not valid')
            #     # raise OSError()
