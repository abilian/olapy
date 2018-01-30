"""
Parse cube configuration file and create cube parser object which
can be passed to the MdxEngine.
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
from collections import OrderedDict

import yaml

from .models import Cube, Dimension, Facts


class ConfigParser:
    """Parse olapy config excel file.

    Config file used if you want to show only some measures, dimensions,
    columns... in excel

    Config file should be under '~/olapy-data/cubes/cubes-config.xml'

    Assuming we have two tables as follows under 'labster' folder

    table 1: stats_line (which is the facts table)

    +----------------+---------+--------------------+----------------------+
    | departement_id | amount  |    monthly_salary  |  total monthly cost  |
    +----------------+---------+--------------------+----------------------+
    |  111           |  1000   |      2000          |    3000              |
    +----------------+---------+--------------------+----------------------+
    | bla  bla bla   |         |                    |                      |
    +----------------+---------+--------------------+----------------------+

    table 2: orgunit (which is a dimension)

    +------+---------------+-----------+------------------+------------------+
    | id   | type          |  name     |  acronym         | other colums.....|
    +------+---------------+-----------+------------------+------------------+
    |  111 | humanitarian  |  humania  | for better life  |                  |
    +------+---------------+-----------+------------------+------------------+
    | bla  | bla   bla     |           |                  |                  |
    +------+---------------+-----------+------------------+------------------+


    Excel Config file Structure example::

        name: foodmart_with_config            # csv folder name or db name
        source: csv                           # csv | postgres | mysql ...

        xmla_authentication : False            need to enter special token with xmla url or not \
                                              (example http://127.0.0.1:8000/xmla?admin)  admin just an example

        facts:
          table_name: food_facts              # facts table name
          keys:
            columns_names:                    # primary keys
              - product_id
              - warehouse_id
              - store_id
            refs:                             # keys refs (example : product_id ref to column id from table Product...)
              - Product.id
              - Warehouse.id
              - Store.id

          measures:                           # list of measures
                                              # by default, all number type columns in facts table
            - units_ordered
            - units_shipped
            - supply_time

        dimensions:                          # star building customized dimensions display in excel from the star schema
          - dimension:
            #  IMPORTANT: put here facts table also (little bug)
              name: food_facts
              displayName: food_facts

          - dimension:
              name: Product
              displayName: Product
              columns:
                - name: id
                - name: brand_name
                - name: product_name
                - name: SKU
                  column_new_name: Stock_keeping_unit


          - dimension :
              name: Store
              displayName: Store
              columns:
                - name: id
                - name: store_type
                - name: store_name
                - name: store_city
                - name: store_country
                  column_new_name: country

          - dimension:
              name: Warehouse
              displayName: Warehouse
              columns:
                - name: id
                - name: warehouse_name
                - name: warehouse_city
                - name: warehouse_country
    """

    def __init__(self, cube_config_file=None):
        """

        :param cube_path: path to cube (csv folders) where config file is located by default
        :param file_name: config file name (DEFAULT = cubes-config.xml)
        :param web_config_file_name: web config file name (DEFAULT = web_cube_config.xml)
        """

        if cube_config_file:
            self.cube_config_file = cube_config_file
        else:
            self.cube_config_file = self._get_cube_path()

    def _get_cube_path(self):
        if 'OLAPY_PATH' in os.environ:
            home_directory = os.environ['OLAPY_PATH']
        else:
            from os.path import expanduser
            home_directory = expanduser("~")

        return os.path.join(home_directory, 'olapy-data', 'cubes', 'cubes-config.yml')

    # XXX: never used. Do we need this?
    def config_file_exists(self):
        # type: () -> bool
        """
        Check whether the config file exists or not.
        """
        return os.path.isfile(self.cube_config_file)

    def _get_dimensions(self, config):
        return [
            Dimension(
                name=dimension['dimension']['name'],
                displayName=dimension['dimension']['displayName'],
                columns=OrderedDict(
                    (
                        column['name'],
                        column['name'] if 'column_new_name' not in column else
                        column['column_new_name'],
                    ) for column in dimension['dimension']['columns']
                ) if 'columns' in dimension['dimension'] else {}
            ) for dimension in config['dimensions']
        ]

    def _get_facts(self, config):
        return [
            Facts(
                table_name=config['facts']['table_name'],
                keys=dict(zip(config['facts']['keys']['columns_names'],
                              config['facts']['keys']['refs'])
                          ),
                measures=config['facts']['measures']
            )
        ]

    def get_cube_config(self, conf_file=None):
        """
        Construct parser cube obj (which can ben passed to MdxEngine) for excel

        :return: Cube obj
        """

        if conf_file:
            file_path = conf_file
        else:
            file_path = self.cube_config_file

        with open(file_path) as config_file:
            config = yaml.load(config_file)

        # only one cube right now
        return Cube(
            xmla_authentication=bool(config['xmla_authentication']),
            name=config['name'],
            source=config['source'],
            facts=self._get_facts(config),
            dimensions=self._get_dimensions(config),
        )
