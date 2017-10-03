from cpuinfo import cpuinfo
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from olap.xmla import xmla
import datetime
from micro_bench import MicBench
from olapy.core.services.xmla import XmlaProviderService
from spyne import Application
from prettytable import PrettyTable

from tests.test_xmla import WSGIServer

HOST = "127.0.0.1"
PORT = 8230
CUBE_NAME = 'sales'

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

    t = PrettyTable(['Query', 'olapy execution time'])

    cmd = """
               SELECT
               FROM [""" + CUBE_NAME + """]
               WHERE ([Measures].[Amount])
               CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

    file.write(
        "Query 1 :\n" + cmd +
        "\n----------------------------------------------------------\n\n")
    t.add_row(['Query 1', mbench.bench(conn, cmd, CUBE_NAME)])

    file.write(str(t) + "\n\n")




if __name__ == '__main__':
    main()