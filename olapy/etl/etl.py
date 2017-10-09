import bonobo
from bonobo.commands.run import get_default_services

from olapy.core.mdx.executor.execute import MdxEngine

class ETL(object):

    def __init__(self,source_type):
        self.source_type = source_type
        self.cube_path = MdxEngine._get_default_cube_directory()
        self.seperator = self._get_default_seperator()

    def _get_default_seperator(self):
        if self.source_type.upper() in ['CSV','FILE']:
            return ','



    def transform(self,line):
        """

        :param table_type: facts | dimension
        :return:
        """
        return line.split(self.seperator)

    def extract(self, file):
        """

        :param file: file | csv | json | pickle
        :return:
        """
        return getattr(bonobo, self.source_type.title() + "Reader")(file)

    def load(self):
        # if not os.path.isdir(os.path.join(self.cube_path, CUBE_NAME)):
        #     os.makedirs(os.path.join(self.cube_path, CUBE_NAME))
        # cube_path = os.path.join(self.cube_path, CUBE_NAME)
        # for (table_name, table_value) in tables.items():
        #     table_value.to_csv(
        #         os.path.join(os.path.join(cube_path, table_name + '.csv')),
        #         sep=";",
        #         index=False)

        return bonobo.CsvWriter('coffeeshops.csv')
        # return  bonobo.JsonWriter('coffeeshops.json', fs='fs.output', ioformat='arg0')

etl = ETL('file')


if __name__ == '__main__':

    graph = bonobo.Graph(
        # etl.extract('coffeeshops.txt'),
        bonobo.CsvReader('coffeeshops.txt'),
        # etl.transform,
        # bonobo.CsvWriter('coffeeshops.csv')
        etl.load()
    )

    def get_services():
        return {
            'fs': bonobo.open_examples_fs('datasets'),
            'fs.output': bonobo.open_fs(),
        }

    bonobo.run(graph, services=get_default_services(__file__))
    # bonobo.run(graph, services=get_services())
