import bonobo
import os

# from bonobo.config import Configurable, Option

from olapy.core.mdx.executor.execute import MdxEngine

# class Transform(Configurable):
#     headers =  Option(list)
#     def call(self, line):
#         transformed = {}
#         splited = line.split(',', maxsplit=len(self.headers))
#         for idx, head in enumerate(self.headers):
#             transformed.update({head: splited[idx]})
#         return transformed


class ETL(object):
    def __init__(self,
                 source_type,
                 facts_table,
                 facts_ids,
                 seperator=None,
                 target_cube='etl_cube',
                 **kwargs):
        """

        :param source_type: csv | file | pickle
        :param headers:
        """
        self.source_type = source_type
        self.facts_table = facts_table
        self.cube_path = MdxEngine._get_default_cube_directory()
        self.seperator = self._get_default_seperator(
        ) if not seperator else seperator
        self.target_cube = target_cube
        self.olapy_cube_path = os.path.join(
            MdxEngine._get_default_cube_directory(), MdxEngine.CUBE_FOLDER)
        # pass some data to transform without bonobo shitty configuration
        self.current_dim_id_column = None
        self.dim_first_row_headers = True
        self.dim_headers = []

    def _get_default_seperator(self):
        if self.source_type.upper() in ['CSV', 'FILE']:
            return ','

    def transform(self, line):
        """

        :param table_type: facts | dimension
        :return:
        """

        transformed = {}

        if self.dim_first_row_headers:
            # split headers
            splited = line.split(self.seperator)
            self.dim_headers = splited
            self.dim_first_row_headers = False
            for idx, column_header in enumerate(splited):
                if column_header == self.current_dim_id_column[0] and '_id' not in column_header[-3:]:
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

    def extract(self, file):
        """

        :param file: file | csv | json | pickle
        :return:
        """
        return getattr(bonobo, self.source_type.title() + "Reader")(file)

    def load(self, table_name, target='csv'):

        if not os.path.isdir(
                os.path.join(self.olapy_cube_path, MdxEngine.CUBE_FOLDER,
                             self.target_cube)):
            os.makedirs(
                os.path.join(self.olapy_cube_path, MdxEngine.CUBE_FOLDER,
                             self.target_cube))

        self.target_cube = os.path.join(self.olapy_cube_path, self.target_cube)
        # todo target postgres, mysql ....
        if target.upper() == 'CSV':
            # todo headers if there is not headers
            # os.chdir(self.target_cube)
            return bonobo.CsvWriter(table_name + '-out.csv', ioformat='arg0')


if __name__ == '__main__':

    # with extension
    dims_infos = {
        # 'dimension': ['col_id'],
        'Geography': ['geokey']
    }

    facts_ids = ['col1', 'col2', 'col3']

    etl = ETL(
        source_type='file',
        facts_table='sales_facts',
        facts_ids=facts_ids,
        **dims_infos)
    for table in list(dims_infos.keys()) + [etl.facts_table]:
        # transform = Transform(dims_infos[table])
        etl.current_dim_id_column = dims_infos[
            table] if table != etl.facts_table else etl.facts_table

        graph = bonobo.Graph(
            etl.extract(table + '.txt'),
            # transform,
            etl.transform,
            etl.load(table))

        bonobo.run(graph)
