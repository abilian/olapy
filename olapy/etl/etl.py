from __future__ import absolute_import, division, print_function, unicode_literals
from shutil import copyfile

import shutil
from bonobo_sqlalchemy import Select
from distutils.dir_util import copy_tree
from olapy.core.mdx.executor.execute import MdxEngine
from bonobo.commands.run import get_default_services

import bonobo
import dotenv
import logging
import os

dotenv.load_dotenv(dotenv.find_dotenv())
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

GEN_FOLDER = 'etl_generated'
INPUT_DIR = 'input_dir'


class ETL(object):
    def __init__(self,
                 source_type,
                 facts_table,
                 source_folder=INPUT_DIR,
                 separator=None,
                 target_cube='etl_cube'):
        """

        :param source_type: file | csv
        :param facts_table: facts table name
        :param source_folder: your csv|txt files path
        :param separator: file separator (, ; : ...)
        :param target_cube: generated file path
        """
        self.source_type = source_type
        self.facts_table = facts_table
        self.cube_path = MdxEngine._get_default_cube_directory()
        self.seperator = self._get_default_seperator(
        ) if not separator else separator
        self.target_cube = target_cube
        if source_folder != INPUT_DIR and source_type.upper() != 'DB':
            # #1 fix bonobo read from file path
            if not os.path.exists(INPUT_DIR):
                os.mkdir(INPUT_DIR)
            for file in os.listdir(INPUT_DIR):
                os.remove(os.path.join(INPUT_DIR, file))
            copy_tree(source_folder, os.path.join(INPUT_DIR))
            self.source_folder = INPUT_DIR
        else:
            self.source_folder = INPUT_DIR
        self.olapy_cube_path = os.path.join(
            MdxEngine._get_default_cube_directory(), MdxEngine.CUBE_FOLDER)
        self.current_dim_id_column = None
        self.dim_first_row_headers = True
        self.dim_headers = []
        if not os.path.exists(GEN_FOLDER):
            os.mkdir(GEN_FOLDER)
        self.services = get_default_services(__file__)

    def _get_default_seperator(self):
        if self.source_type.upper() in ['CSV', 'FILE']:
            return ','

    def transform_file(self, line):
        """

        :param table_type: facts | dimension
        :return:
        """
        line = line[0]
        transformed = {}

        if self.dim_first_row_headers:
            # split headers
            splited = line.split(self.seperator)
            self.dim_headers = splited
            self.dim_first_row_headers = False
            for idx, column_header in enumerate(splited):
                if column_header in self.current_dim_id_column and '_id' not in column_header[-3:]:
                    splited[idx] = column_header + '_id'

        else:
            if self.dim_headers:
                splited = line.split(
                    self.seperator, maxsplit=len(self.dim_headers))
            else:
                # columns = self.current_dim_id_column
                splited = line.split(self.seperator)

            for idx, head in enumerate(self.dim_headers):
                transformed.update({head: splited[idx]})

        return transformed

    def transform_csv(self, kwargs):
        if self.dim_first_row_headers:
            for key in self.current_dim_id_column:
                if '_id' not in key:
                    kwargs[key + '_id'] = kwargs[key]
                    del kwargs[key]
        return kwargs

    def transform(self, *args, **kwargs):
        """

        :param table_type: facts | dimension
        :return:
        """
        if self.source_type.upper() == 'FILE':
            return self.transform_file(args)

        elif self.source_type.upper() == 'CSV':
            return self.transform_csv(kwargs)
        else:
            return args if args else kwargs

    def extract(self, file, **kwargs):
        """

        :param file: file | csv
        :return:
        """
        if self.source_type.upper() == 'DB':
            return Select('SELECT * from {};'.format(file))
        elif self.source_type.upper() == 'FILE':
            # delimiter not used with files
            kwargs.pop('delimiter', None)
        return getattr(bonobo, self.source_type.title() + "Reader")(file, **kwargs)

    def load(self, table_name):
        """

        :param table_name: table name to generate
        :return: generated table into olapy dir
        """
        if table_name == self.facts_table:
            table_name = 'Facts'
        return bonobo.CsvWriter(
            os.path.join(GEN_FOLDER, table_name + '.csv'), ioformat='arg0')

    def copy_2_olapy_dir(self):
        """
        bonobo can't export (save) to path (bonobo bug) so we copy all generated tables directly to olapy dir
        :return:
        """
        if not os.path.isdir(
                os.path.join(self.olapy_cube_path, self.target_cube)):
            os.makedirs(os.path.join(self.olapy_cube_path, self.target_cube))

        self.target_cube = os.path.join(self.olapy_cube_path, self.target_cube)

        for file in os.listdir(GEN_FOLDER):
            copyfile(
                os.path.join(GEN_FOLDER, file),
                os.path.join(self.target_cube, file))

    def get_source_extension(self):
        """
        get source file extension
        :return: .txt | .csv
        """
        if self.source_type.upper() == 'FILE':
            return '.txt'
        elif self.source_type.upper() == 'CSV':
            return '.csv'
        elif self.source_type.upper() == 'DB':
            return ''


def run_olapy_etl(dims_infos,
                  facts_table,
                  facts_ids,
                  source_folder=INPUT_DIR,
                  source_type='csv',
                  in_delimiter=',',
                  **kwargs):
    """

    :param dims_infos: example : dims_infos = {
                                            'Geography': ['geography_key'],
                                            'Product': ['product_key']
                                             }
    :param facts_table: facts table name
    :param facts_ids: example : facts_ids = ['geography_key', 'product_key']
    :param source_folder: from where you get your files
    :param source_type: file -> .txt files in input || csv -> .csv files in input
    :return: generate files to olapy dir
    """

    # source_type -> file : .txt files in input
    # source_type -> csv : .csv files in input
    etl = ETL(
        source_type=source_type,
        facts_table=facts_table,
        source_folder=source_folder)

    for table in list(dims_infos.keys()) + [etl.facts_table]:
        if etl.source_type.upper() != 'DB':
            extraction_source = os.path.join(
                etl.source_folder, table + etl.get_source_extension())
        else:
            extraction_source = table

        # for each new file
        etl.dim_first_row_headers = True
        if table == etl.facts_table:
            etl.current_dim_id_column = facts_ids
        else:
            etl.current_dim_id_column = dims_infos[table]
        graph = bonobo.Graph(
            etl.extract(extraction_source, delimiter=in_delimiter),
            etl.transform, etl.load(table))
        bonobo.run(graph, services=etl.services)

    # temp ( bonobo can't export (save) to path (bonobo bug)
    etl.copy_2_olapy_dir()
    if os.path.isdir(GEN_FOLDER):
        shutil.rmtree(GEN_FOLDER)
