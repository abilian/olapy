from os.path import expanduser

import shutil

import sys
from cpuinfo import cpuinfo, os
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from olap.xmla import xmla
import datetime
from micro_bench import MicBench
from olapy.core.services.xmla import XmlaProviderService
from spyne import Application
from prettytable import PrettyTable

from queries_4_db import query9, query7, query6, query1
from tests.test_xmla import WSGIServer

HOST = "127.0.0.1"
PORT = 8230
CUBE_NAME = 'sales'


def get_olapy_dir():
    if 'OLAPY_PATH' in os.environ:
        home_directory = os.environ.get('OLAPY_PATH')
    else:
        home_directory = expanduser("~")

    return os.path.join(home_directory, 'olapy-data')


def copy_2_olapy_dir(config_file):
    olapy_dir = get_olapy_dir()
    shutil.copy(os.path.join(os.getcwd(), 'db_config_files', config_file), os.path.join(olapy_dir, 'olapy-config.xml'))

# def fix_query_for_postges(query):
#     # for postgress
#     query.replace('Amount', 'amount')
#     query.replace('Time', 'time')
#     query.replace('Year', 'year')
#     query.replace('Quarter', 'quarter')
#     query.replace('Month', 'month')
#     query.replace('Product','product')
#     query.replace('Company', 'company')
#     query.replace('Day', 'day')
#     query.replace('Geography', 'geography')
#     query.replace('Continent', 'continent')



def main():
    file = open('DATABASES_bench_result' +
                str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")), 'w')

    file.write("Benchmarks are made with cpu :\n")
    file.write(cpuinfo.get_cpu_info()['brand'] + "\n\n")

    olapy_dir = get_olapy_dir()
    if os.path.isfile(os.path.join(olapy_dir, 'olapy-config.xml')):
        os.rename(os.path.join(olapy_dir, 'olapy-config.xml'), os.path.join(olapy_dir, 'backup_olapy-config.xml'))

    for config_file in os.listdir(os.path.join(os.getcwd(), 'db_config_files')):

        sgbd = str(config_file.split('_')[0])
        try:
            copy_2_olapy_dir(config_file)

            application = Application(
                [XmlaProviderService],
                'urn:schemas-microsoft-com:xml-analysis',
                in_protocol=Soap11(validator='soft'),
                out_protocol=Soap11(validator='soft'))
            wsgi_application = WsgiApplication(application)
            server = WSGIServer(application=wsgi_application, host=HOST, port=PORT)
            server.start()

            provider = xmla.XMLAProvider()
            conn = provider.connect(location=server.url)

            mbench = MicBench()
            XmlaProviderService.discover_tools.change_catalogue(CUBE_NAME)

            t = PrettyTable(['Query', '{0} - olapy execution time'.format(sgbd)])

            for idx, query in enumerate([query1, query6, query7, query9]):
                file.write(
                    "Query {0} :\n".format(str(idx + 1)) + query +
                    "\n----------------------------------------------------------\n\n")

            for idx, query in enumerate([query1, query6, query7, query9]):
                t.add_row(['Query' + str(idx + 1), str(mbench.bench(conn, query, CUBE_NAME))])

            server.stop()
            file.write(str(t) + "\n\n")
        except:
            type, value, traceback = sys.exc_info()
            print('Error opening %s' % (value))
            print("Can't connect to the database")
            pass



if __name__ == '__main__':
    main()
