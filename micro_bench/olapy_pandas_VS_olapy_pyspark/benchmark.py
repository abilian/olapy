from __future__ import absolute_import, division, print_function, \
    unicode_literals

import subprocess
from importlib import reload
from pprint import pprint

from olapy.core.mdx.executor import MdxEngine
from olapy.core.services import XmlaDiscoverReqHandler, XmlaExecuteReqHandler

from .benchmark_tools import BENCH_CUBE, run_benchmark, save_benchmark_result


def benchmark():
    mdx_executor = MdxEngine(
        cubes_folder="cubes_templates", olapy_data_location="../../../olapy"
    )
    mdx_executor.load_cube(BENCH_CUBE)
    xmla_discover_request_handler = XmlaDiscoverReqHandler(mdx_executor)
    xmla_execute_request_handler = XmlaExecuteReqHandler(mdx_executor)

    return run_benchmark(
        mdx_executor, xmla_discover_request_handler, xmla_execute_request_handler
    )


if __name__ == "__main__":

    bench_result = {"pandas": benchmark()}

    try:

        subprocess.call("pip install pyspark", shell=True)

        # force python to reimport MdxEngine (as SparkMdxEngine)
        import olapy

        reload(olapy.core.mdx.executor)
        reload(olapy.core.services)
        from olapy.core.mdx.executor import MdxEngine
        from olapy.core.services import XmlaDiscoverReqHandler
        from olapy.core.services import XmlaExecuteReqHandler

        bench_result["spark"] = benchmark()

    except Exception as e:
        print(e)
    finally:
        subprocess.call("pip uninstall pyspark -y", shell=True)

    pprint(bench_result)

    save_benchmark_result(bench_result)
