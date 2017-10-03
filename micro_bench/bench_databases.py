from cpuinfo import cpuinfo
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from olap.xmla import xmla
import datetime
from micro_bench import MicBench
from olapy.core.services.xmla import XmlaProviderService
from spyne import Application
from prettytable import PrettyTable

from tests.queries import query9, query7, query6, query1
from tests.test_xmla import WSGIServer

HOST = "127.0.0.1"
PORT = 8230
CUBE_NAME = 'test1'

def main():

    mbench = MicBench()
    XmlaProviderService.discover_tools.change_catalogue(CUBE_NAME)

    file = open('DATABASES_bench_result' +
                str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")), 'w')

    file.write("Benchmarks are made with cpu :\n")
    file.write(cpuinfo.get_cpu_info()['brand'] + "\n\n")

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

    t = PrettyTable(['Query', 'MYSQL - olapy execution time'])

    for idx, query in enumerate([query1, query6, query7, query9]):
        file.write(
            "Query 1 :\n" + query +
            "\n----------------------------------------------------------\n\n")
        t.add_row(['Query' + str(idx + 1), mbench.bench(conn, query, CUBE_NAME)])

    file.write(str(t) + "\n\n")


if __name__ == '__main__':
    main()