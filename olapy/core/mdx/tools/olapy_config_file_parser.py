from __future__ import absolute_import, division, print_function, unicode_literals

import os

import yaml


class DbConfigParser:
    def __init__(self, config_path=None, file_name='olapy-config'):
        """

        :param cube_path: path to cube (csv folders)
        :param file_name: config file name (DEFAULT = cubes-config.xml)
        """
        if config_path is None:
            from os.path import expanduser
            home_directory = expanduser("~")
            self.cube_path = os.path.join(home_directory, 'olapy-data')
        else:
            self.cube_path = config_path
        self.file_name = file_name

    def config_file_exist(self):
        """
        Check whether the config file exists or not.

        :return: True | False
        """
        return os.path.isfile(os.path.join(self.cube_path, self.file_name))

    def get_db_credentials(self):
        """
        Get all db credentials in the config file.

        :return: list of cube name as key and cube source as value (csv or postgres) (right now only postgres is supported)
        """
        with open(os.path.join(self.cube_path, self.file_name)) as config_file:
            try:
                config = yaml.load(config_file)
                return {
                    'sgbd': config['sgbd'],
                    'user_name': config['user'],
                    'password': config['password'] if 'LOCALHOST' not in config['user'].upper() else '',
                    'host': config['host'] if 'LOCALHOST' not in config['user'].upper() else '',
                    'port': config['port'],
                    'db_name': config['db_name'] if 'db_name' in config.keys() else '',
                    'driver': config['driver'] if 'driver' in config.keys() else ''
                }

            except OSError:
                print('olapy db_config not valid')
                # raise OSError()
