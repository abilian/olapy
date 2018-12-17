from __future__ import absolute_import, division, print_function, unicode_literals

import subprocess
from pprint import pprint

from benchmark_tools import run_benchmark, plot_bar_chart, save_benchmark_result, BENCH_CUBE


def benchmark_pandas():
    from olapy.core.mdx.executor.execute import MdxEngine
    from olapy.core.services.xmla_execute_request_handler import XmlaExecuteReqHandler
    from olapy.core.services.xmla_discover_request_handler import XmlaDiscoverReqHandler

    mdx_executor = MdxEngine(cubes_folder='cubes_templates', olapy_data_location='../../../olapy')
    mdx_executor.load_cube(BENCH_CUBE)
    Xmla_discover_request_handler = XmlaDiscoverReqHandler(mdx_executor)
    Xmla_execute_request_handler = XmlaExecuteReqHandler(mdx_executor)

    return run_benchmark(mdx_executor, Xmla_discover_request_handler, Xmla_execute_request_handler)


def benchmark_spark():
    try:
        subprocess.call('pip install pyspark', shell=True)

        from olapy.core.mdx.executor.spark.execute import SparkMdxEngine
        from olapy.core.services.spark.xmla_discover_request_handler import SparkXmlaDiscoverReqHandler
        from olapy.core.services.spark.xmla_execute_request_handler import SparkXmlaExecuteReqHandler

        spark_mdx_executor = SparkMdxEngine(cubes_folder='cubes_templates', olapy_data_location='../../../olapy')
        spark_mdx_executor.load_cube(BENCH_CUBE)

        Xmla_discover_request_handler = SparkXmlaDiscoverReqHandler(spark_mdx_executor)
        Xmla_execute_request_handler = SparkXmlaExecuteReqHandler(spark_mdx_executor)
        return run_benchmark(spark_mdx_executor, Xmla_discover_request_handler, Xmla_execute_request_handler)
    except Exception as e:
        print(e)
    finally:
        subprocess.call('pip uninstall pyspark -y', shell=True)


if __name__ == '__main__':
    pandas_benchmark = benchmark_pandas()
    spark_benchmark = benchmark_spark()

    bench_result = {
        'pandas': pandas_benchmark,
        'spark': spark_benchmark
    }

    pprint(bench_result)

    save_benchmark_result(bench_result)
