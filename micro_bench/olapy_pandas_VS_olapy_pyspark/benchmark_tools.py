from __future__ import absolute_import, division, print_function, \
    unicode_literals

import datetime
import threading
from timeit import Timer
from wsgiref.simple_server import make_server

from cpuinfo import cpuinfo
from olap.xmla import xmla
from prettytable import PrettyTable
from spyne.server.wsgi import WsgiApplication

from mdx_queries import query1, query3, query2

from olapy.core.services.xmla import get_spyne_app

import numpy as np
import matplotlib.pyplot as plt

HOST = "127.0.0.1"
PORT = 8000
BENCH_CUBE = 'foodmart'


class WSGIServer:
    """HTTP server running a WSGI application in its own thread.

    Copy/pasted from pytest_localserver w/ slight changes.
    """

    def __init__(self, host='127.0.0.1', port=8000, application=None, **kwargs):
        self._server = make_server(host, port, application, **kwargs)
        self.server_address = self._server.server_address

        self.thread = threading.Thread(name=self.__class__, target=self._server.serve_forever)

    def __del__(self):
        self.stop()

    def start(self):
        self.thread.start()

    def stop(self):
        self._server.shutdown()
        self.thread.join()

    @property
    def url(self):
        host, port = self.server_address
        proto = 'http'  # if self._server.ssl_context is None else 'https'
        return '{}://{}:{}'.format(proto, host, port)


def olapy_mdx_benchmark(queries, mdx_engine):
    return [Timer(lambda: mdx_engine.execute_mdx(query)).timeit(
        number=1)
        for query in queries]


def olapy_xmla_benchmark(queries):
    provider = xmla.XMLAProvider()
    connection = provider.connect(location='http://{0}:{1}'.format(HOST, PORT))

    return [Timer(lambda: connection.Execute(query, Catalog=BENCH_CUBE)).timeit(
        number=1) for query in queries]


def get_olapy_server(Xmla_discover_request_handler, Xmla_execute_request_handler):
    application = get_spyne_app(Xmla_discover_request_handler, Xmla_execute_request_handler)
    wsgi_application = WsgiApplication(application)
    return WSGIServer(application=wsgi_application, host=HOST, port=PORT)


def run_benchmark(mdx_engine, Xmla_discover_request_handler, Xmla_execute_request_handler):
    results = {}
    server = get_olapy_server(Xmla_discover_request_handler, Xmla_execute_request_handler)
    server.start()

    queries = [query1, query2, query3]

    results["mdx"] = olapy_mdx_benchmark(queries, mdx_engine)
    results["xmla"] = olapy_xmla_benchmark(queries)

    server.stop()

    return results


def draw_table(bench_result, of='mdx'):
    table = PrettyTable(['', 'Pandas', 'Spark'])
    for idx, row in enumerate(bench_result['pandas'][of]):
        table.add_row([
            'Query ' + (str(idx + 1)),
            bench_result['pandas'][of][idx],
            bench_result.get('spark', '')[of][idx]
        ])
    return table


def save_benchmark_result(bench_result):
    plot_bar_chart(bench_result, of='mdx', label="MDX Execution time", title="Execution time by mdx query",
                   out_file="img/pandas_spark_mdx_exec.png")
    plot_bar_chart(bench_result, of='xmla', label='XMLA execution time', title='XMLA execution by query',
                   out_file="img/pandas_spar_xmla_exec.png")

    with open('benchmark_result-{0}.rst'.format(
        str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"))), 'w+') as file:
        file.write("Benchmarks are made with cpu :\n")
        file.write(cpuinfo.get_cpu_info()['brand'] + "\n\n")
        file.write(str(draw_table(bench_result, 'mdx')))
        file.write('\n\n .. image:: img/pandas_spark_mdx_exec.png')
        file.write('\n\n')
        file.write(str(draw_table(bench_result, 'xmla')))
        file.write('\n\n .. image:: img/pandas_spar_xmla_exec.png')


def plot_bar_chart(bench_result, of='mdx', label="Execution time", title="Execution time by mdx query",
                   out_file="img/pandas_spark_mdx_exec.png"):
    pandas_exec_time = bench_result['pandas'][of]
    Spark_exec_time = bench_result.get('spark', '')[of]

    ind = np.arange(len(pandas_exec_time))  # the x locations for the groups
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    ax.bar(ind - width / 2, pandas_exec_time, width,
           color='SkyBlue', label='Pandas')
    ax.bar(ind + width / 2, Spark_exec_time, width,
           color='IndianRed', label='Spark')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel(label)
    ax.set_title(title)
    ax.set_xticks(ind)
    ax.set_xticklabels(['Q' + str(idx + 1) for idx, val in enumerate(bench_result['pandas'][of])])
    ax.legend()

    # plt.show()
    plt.savefig(out_file)
