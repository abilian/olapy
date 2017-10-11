import bonobo
import os

from olapy.core.mdx.executor.execute import MdxEngine


class ETL(object):
    def __init__(self, source_type, headers, target_cube='etl_cube'):
        """

        :param source_type: csv | file | pickle
        :param headers:
        """
        self.source_type = source_type
        self.cube_path = MdxEngine._get_default_cube_directory()
        self.seperator = self._get_default_seperator()
        self.headers = headers if headers else []
        self.target_cube = target_cube
        self.olapy_cube_path = os.path.join(MdxEngine._get_default_cube_directory())
        if not os.path.isdir(os.path.join(self.olapy_cube_path, MdxEngine.CUBE_FOLDER, self.target_cube)):
            os.makedirs(os.path.join(self.olapy_cube_path, MdxEngine.CUBE_FOLDER, self.target_cube))

    def _get_default_seperator(self):
        if self.source_type.upper() in ['CSV', 'FILE']:
            return ','

    def transform(self, line):
        """

        :param table_type: facts | dimension
        :return:
        """
        # return line
        transformed = {}
        splited = line.split(self.seperator, maxsplit=len(self.headers))
        for idx, head in enumerate(self.headers):
            transformed.update({head: splited[idx]})
        return transformed

    def extract(self, file, facts=False):
        """

        :param file: file | csv | json | pickle
        :return:
        """
        return getattr(bonobo, self.source_type.title() + "Reader")(file)

    def load(self, target='csv'):
        self.target_cube = os.path.join(self.olapy_cube_path, self.target_cube)

        # todo target postgres, mysql ....
        if target.upper() == 'CSV':
            return bonobo.CsvWriter('coffeeshops.csv', ioformat='arg0')


etl = ETL(source_type='file', headers=['col1', 'col2', 'col3', 'col4'])

if __name__ == '__main__':
    graph = bonobo.Graph(
        etl.extract('test.txt'),
        etl.transform,
        etl.load())

    bonobo.run(graph)
