"""OlaPy config file parser object contains credentials that allows olapy to
access database (of course this happens only if SQLALCHEMY_DATABASE_URI
Environment variable is not specified)."""

import yaml

from olapy.core.common import DEFAULT_CONFIG  # .../olapy-data/olapy-config.yml


class DbConfigParser:
    """Class to construct to olapy database config object."""

    def __init__(self, config_file_path=None):
        """
        :param config_file_path: full path to olapy config file,
            Default : ~/olapy-data/olapy-config.yml
        """
        if config_file_path is None:
            config_file_path = DEFAULT_CONFIG
        with open(config_file_path, encoding="utf8") as config_file:
            self.config = yaml.load(config_file, Loader=yaml.SafeLoader)

    @property
    def db_credentials(self):
        """Get all db credentials in the config file.

        :return: dict of database connection credentials.
        """
        return self.config["connection_string"]
