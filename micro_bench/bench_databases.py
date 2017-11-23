from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import shutil
import sys
from os.path import expanduser

from cpuinfo import cpuinfo, os
from olap.xmla import xmla
from prettytable import PrettyTable
from spyne import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from tests.queries import query1, query6, query7, query9
from tests.test_xmla import WSGIServer

from micro_bench import MicBench
from olapy.core.services.xmla import XmlaProviderService
from olapy.core.services.xmla_discover_tools import XmlaDiscoverTools

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
    shutil.copy(
        os.path.join(os.getcwd(), 'db_config_files', config_file),
        os.path.join(olapy_dir, 'olapy-config'))


def fix_query_lowercase_db(query):
    # for postgres table names and columns are lowercase
    return query.replace('Amount', 'amount').replace('Time', 'time').replace(
        'Year',
        'year').replace('Quarter', 'quarter').replace('Month', 'month').replace(
            'Product', 'product').replace('Company', 'company').replace(
                'Day', 'day').replace('Geography', 'geography').replace(
                    'Continent', 'continent')


def backup_config_file():
    olapy_dir = get_olapy_dir()
    if os.path.isfile(os.path.join(olapy_dir, 'olapy-config')):
        os.rename(
            os.path.join(olapy_dir, 'olapy-config'),
            os.path.join(olapy_dir, 'backup_olapy-config'))


def restore_config_file():
    olapy_dir = get_olapy_dir()
    if os.path.isfile(os.path.join(olapy_dir, 'backup_olapy-config')):
        os.rename(
            os.path.join(olapy_dir, 'backup_olapy-config'),
            os.path.join(olapy_dir, 'olapy-config'))
        os.remove(os.path.join(olapy_dir, 'backup_olapy-config'))


def main():
    file = open('DATABASES_bench_result' +
                str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")), 'w')

    file.write("Benchmarks are made with cpu :\n")
    file.write(cpuinfo.get_cpu_info()['brand'] + "\n\n")

    for idx, query in enumerate([query1, query6, query7, query9]):
        file.write(
            "Query {0} :\n".format(str(idx + 1)) + query +
            "\n----------------------------------------------------------\n\n")

    application = Application(
        [XmlaProviderService],
        'urn:schemas-microsoft-com:xml-analysis',
        in_protocol=Soap11(validator='soft'),
        out_protocol=Soap11(validator='soft'))
    wsgi_application = WsgiApplication(application)
    server = WSGIServer(application=wsgi_application, host=HOST, port=PORT)

    server.start()

    for config_file in os.listdir(os.path.join(os.getcwd(), 'db_config_files')):

        dbms = str(config_file.split('_')[0])

        try:
            copy_2_olapy_dir(config_file)

            # to refresh cubes from database
            XmlaProviderService.discover_tools = XmlaDiscoverTools()

            provider = xmla.XMLAProvider()
            conn = provider.connect(location=server.url)

            mbench = MicBench()
            XmlaProviderService.discover_tools.change_catalogue(CUBE_NAME)

            t = PrettyTable(
                ['Query', '{0} - olapy execution time'.format(dbms)])

            for idx, query in enumerate([query1, query6, query7, query9]):

                if dbms.upper() in ['POSTGRES', 'ORACLE']:
                    query = fix_query_lowercase_db(query)
                t.add_row([
                    'Query' + str(idx + 1),
                    str(mbench.bench(conn, query, CUBE_NAME))
                ])

            file.write(str(t) + "\n\n")
        except:
            type, value, traceback = sys.exc_info()
            print('Error opening %s' % (value))
            print("Can't connect to {0} database".format(dbms))
            pass
    server.stop()


if __name__ == '__main__':
    main()
