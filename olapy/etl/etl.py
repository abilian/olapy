"""
To use olapy correctly with you source file (tables), some data structure
cleaning operation must be applied to that data (for example, if you want
to use olapy with csv files, you must use semicolon separators for those
csv files; or if tables id columns don't contains _id, this will cause some
bugs sometimes).

This module will do the work for you, here olapy will extract data from
your source, transform it with olapy's data structure rules,
and load them to olapy-data folder *(as csv files right now)*.
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging
import os
import shutil
from distutils.dir_util import copy_tree
from shutil import copyfile
from typing import Text

import bonobo
import dotenv

from olapy.core.mdx.executor.execute import MdxEngine
from olapy.etl.utils import create_db_engine

dotenv.load_dotenv(dotenv.find_dotenv())
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

GEN_FOLDER = "etl_generated"
INPUT_DIR = "input_dir"


class ETL(object):
    """
    Extract-transform-load for Olapy.

    It take a source (folder or database), make all necessary transformation to
    that data, and then load them into olapy-data directory.
    """

    def __init__(
            self,
            source_type,
            facts_table,
            source_folder=INPUT_DIR,
            separator=None,
            olay_cubes_path=None,
            target_cube="etl_cube",
    ):
        """

        :param source_type: file | csv | db (if db, make sure that you put
            database connection credentials in the
            /home/user/olapy-data/olapy-config file
        :param facts_table: facts table name
        :param source_folder: your csv|txt files path
        :param separator: input file separator (, ; : ...)
        :param target_cube: generated file path
        """
        self.source_type = source_type
        self.facts_table = facts_table
        if olay_cubes_path:
            self.olapy_cube_path = olay_cubes_path
        else:
            self.olapy_cube_path = os.path.join(
                MdxEngine.get_default_cube_directory(),
                "cubes",
            )
        self.seperator = self._get_default_seperator(
        ) if not separator else separator
        self.target_cube = target_cube
        if source_folder != INPUT_DIR and source_type.upper() != "DB":
            # #1 fix bonobo read from file path
            if not os.path.exists(INPUT_DIR):
                os.mkdir(INPUT_DIR)
            for file in os.listdir(INPUT_DIR):
                os.remove(os.path.join(INPUT_DIR, file))
            copy_tree(source_folder, os.path.join(INPUT_DIR))
            self.source_folder = INPUT_DIR
        else:
            self.source_folder = INPUT_DIR
        self.current_dim_id_column = None
        self.dim_first_row_headers = True
        self.dim_headers = []
        if not os.path.exists(GEN_FOLDER):
            os.mkdir(GEN_FOLDER)

    def _get_default_seperator(self):
        if self.source_type.upper() in ["CSV", "FILE"]:
            return ","

    def _transform_file(self, line):
        """
        :func:`extract` return line by line as args if :func:`extract`
            from text file
        :param line: line is generated from extract function (see bonobo chain)
        :return: dict { column_name : data}
        """

        line = line[0]
        transformed = {}

        if self.dim_first_row_headers:
            # split headers
            splited = line.split(self.seperator)
            self.dim_headers = splited
            self.dim_first_row_headers = False
            for idx, column_header in enumerate(splited):
                if (column_header in self.current_dim_id_column and
                        "_id" not in column_header[-3:]):
                    splited[idx] = column_header + "_id"

        else:
            if self.dim_headers:
                splited = line.split(
                    self.seperator,
                    maxsplit=len(self.dim_headers),
                )
            else:
                # columns = self.current_dim_id_column
                splited = line.split(self.seperator)

            for idx, head in enumerate(self.dim_headers):
                transformed.update({head: splited[idx]})

        return transformed

    def _transform_csv(self, kwargs):
        """
        :func:`extract` return dict as kwargs if :func:`extract` from csv file
        :param kwargs: :func:`extract` return dict as kwargs
            if :func:`extract` from csv file
        :return: transformed kwargs
        """
        if self.dim_first_row_headers:
            for key in self.current_dim_id_column:
                if "_id" not in key:
                    kwargs[key + "_id"] = kwargs[key]
                    del kwargs[key]
        return kwargs

    def extract(self, table_name, **kwargs):
        """
        Bonobo's First chain, extract data from source.

        :param table_name: table/file to extract
        :return: Bonobo Reader
        """
        if self.source_type.upper() == "DB":
            engine = create_db_engine()
            return engine.execute("SELECT * from {};".format(table_name))

        elif self.source_type.upper() == "FILE":
            # delimiter not used with files
            return bonobo.FileReader(table_name)

        elif self.source_type.upper() == "CSV":
            return bonobo.CsvReader(table_name, **kwargs)

    # return getattr(bonobo, self.source_type.title() + "Reader")(table_name, **kwargs)

    def transform(self, *args, **kwargs):
        """
        Bonobo's second chain, transform data based on olapy rules.

        :param args: :func:`extract` return line by line as args
            if :func:`extract` from text file
        :param kwargs: :func:`extract` return dict as kwargs
            if :func:`extract` from csv file
        :return: args or kwargs transformed
        """
        if self.source_type.upper() == "FILE":
            return self._transform_file(kwargs)

        elif self.source_type.upper() == "CSV":
            return self._transform_csv(args)

        else:
            return args if args else kwargs

    def load(self, table_name):
        """
        Bonobo's third chain, load data transformed to olapy-data.

        :param table_name: table name to generate
        :return: generated table into olapy dir
        """
        if table_name == self.facts_table:
            table_name = "Facts"
        return bonobo.CsvWriter(os.path.join(GEN_FOLDER, table_name + ".csv"))

    def copy_2_olapy_dir(self):
        """
        Right now, bonobo can't export (save) to path (bonobo bug) so we copy
        all generated tables directly to olapy dir.
        """
        if not os.path.isdir(
                os.path.join(self.olapy_cube_path, self.target_cube),):
            os.makedirs(os.path.join(self.olapy_cube_path, self.target_cube))

        self.target_cube = os.path.join(self.olapy_cube_path, self.target_cube)

        for file in os.listdir(GEN_FOLDER):
            copyfile(
                os.path.join(GEN_FOLDER, file),
                os.path.join(self.target_cube, file),
            )

    def get_source_extension(self):
        # type: () -> Text
        """
        Get source file extension based on :attr:`self.source_type`.

        :return: .txt OR .csv
        """
        if self.source_type.upper() == "FILE":
            return ".txt"

        elif self.source_type.upper() == "CSV":
            return ".csv"

        elif self.source_type.upper() == "DB":
            return ""

        raise RuntimeError("Unknown source type: {}".format(self.source_type))


def get_graph(etl, **options):
    # (Any) -> bonobo.Graph
    return bonobo.Graph(
        etl.extract(
            options.get("extraction_source"),
            delimiter=options.get("in_delimiter"),
        ),
        etl.transform,
        etl.load(options.get("table")),
    )


def run_olapy_etl(dims_infos,
                  facts_table,
                  facts_ids,
                  source_folder=INPUT_DIR,
                  source_type="csv",
                  in_delimiter=",",
                  **kwargs):
    """
    Run ETl Process on each table pass to it.

    :param dims_infos: dict of Dimension name as key, column id name as value

    example::

        dims_infos = {
                    'Geography': ['geography_key'],
                    'Product': ['product_key']
                     }

    :param facts_ids: list of facts ids

    example::

        facts_ids = ['geography_key', 'product_key']

    :param facts_table: facts table name
    :param source_type: file -> .txt files in input || csv -> .csv files
        in input
    :param source_folder: from where you get your files
        (if source is csv or text files)
    :return: generate files to olapy dir
    """

    # source_type -> file : .txt files in input
    # source_type -> csv : .csv files in input
    etl = ETL(
        source_type=source_type,
        facts_table=facts_table,
        source_folder=source_folder,
    )

    for table in list(dims_infos.keys()) + [etl.facts_table]:
        if etl.source_type.upper() != "DB":
            extraction_source = os.path.join(
                etl.source_folder,
                table + etl.get_source_extension(),
            )
        else:
            extraction_source = table

        # for each new file
        etl.dim_first_row_headers = True
        if table == etl.facts_table:
            etl.current_dim_id_column = facts_ids
        else:
            etl.current_dim_id_column = dims_infos[table]

        # parser = bonobo.get_argument_parser()
        # with bonobo.parse_args(parser):
        bonobo.run(
            get_graph(
                etl,
                extraction_source=extraction_source,
                in_delimiter=in_delimiter,
                table=table,
            ),)

    # temp ( bonobo can't export (save) to path (bonobo bug)
    etl.copy_2_olapy_dir()
    if os.path.isdir(GEN_FOLDER):
        shutil.rmtree(GEN_FOLDER)
