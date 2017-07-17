from __future__ import absolute_import, division, print_function

import os

from lxml import etree


class DbConfigParser:

    # TODO one config file (I will try to merge dimensions between them in web part)
    def __init__(self, config_path=None, file_name='olapy-config.xml'):
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

            parser = etree.XMLParser()
            tree = etree.parse(config_file, parser)

            return [
                {
                    # 'sgbd': db.find('sgbd').text,
                    'user_name': db.find('user_name').text,
                    'password': db.find('password').text,
                    'host': db.find('host').text,
                    'port': db.find('port').text,
                } for db in tree.xpath('/olapy/database')
            ]
