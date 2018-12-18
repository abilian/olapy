from __future__ import absolute_import, division, print_function, \
    unicode_literals

import datetime
from timeit import Timer

from cpuinfo import cpuinfo
from olap.xmla import xmla
from prettytable import PrettyTable
from spyne.server.wsgi import WsgiApplication

from mdx_queries import query1, query3, query2

from tests.test_xmla import WSGIServer

from olapy.core.services.xmla import get_spyne_app

import numpy as np
import matplotlib.pyplot as plt

HOST = "127.0.0.1"
PORT = 8000
BENCH_CUBE = 'foodmart'


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
