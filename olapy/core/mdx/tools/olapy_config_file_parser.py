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
        :param config_file_path: full path to olapy config file,
            Default : ~/olapy-data/olapy-config.yml
        """

        if config_file_path is None:
            from os.path import expanduser

            home_directory = expanduser("~")
            self.config_file_path = os.path.join(
                home_directory,
                "olapy-data",
                "olapy-config.yml",
            )
        else:
            self.config_file_path = config_file_path

    def get_db_credentials(self, db_config_path=None):
        """
        Get all db credentials in the config file.

        :param db_config_path: full path to olapy config file,
            Default : ~/olapy-data/olapy-config.yml
        :return: dict of database connection credentials.
        """
        if db_config_path:
            file_path = db_config_path
        else:
            file_path = self.config_file_path
        with open(file_path) as config_file:
            config = yaml.load(config_file)
            return config["connection_string"]
