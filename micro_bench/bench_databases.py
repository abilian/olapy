from __future__ import absolute_import, division, print_function, \
    unicode_literals

import datetime
import shutil
import sys
from os.path import expanduser
from urllib.parse import urlparse

import sqlalchemy
from cpuinfo import cpuinfo, os
from olap.xmla import xmla
from olap.xmla.interfaces import XMLAException
from prettytable import PrettyTable
from tests.queries import query1, query6, query7, query9
from tests.test_xmla import WSGIServer

from olapy.core.mdx.executor import MdxEngine
from olapy.core.services.xmla import get_wsgi_application

from .micro_bench import MicBench

HOST = "127.0.0.1"
PORT = 8230
CUBE_NAME = "sales"


def get_olapy_dir():
    if "OLAPY_PATH" in os.environ:
        home_directory = os.environ.get("OLAPY_PATH")
    else:
        home_directory = expanduser("~")

    return os.path.join(home_directory, "olapy-data")


def copy_2_olapy_dir(config_file):
    olapy_dir = get_olapy_dir()
    shutil.copy(
        os.path.join(os.getcwd(), "db_config_files", config_file),
        os.path.join(olapy_dir, "olapy-config.yml"),
    )


def fix_query_lowercase_db(query):
    # for postgres table names and columns are lowercase
    return (
        query.replace("Amount", "amount")
        .replace("Time", "time")
        .replace("Year", "year")
        .replace("Quarter", "quarter")
        .replace("Month", "month")
        .replace("Product", "product")
        .replace("Company", "company")
        .replace("Day", "day")
        .replace("Geography", "geography")
        .replace("Continent", "continent")
    )


def backup_config_file():
    olapy_dir = get_olapy_dir()
    if os.path.isfile(os.path.join(olapy_dir, "olapy-config.yml")):
        os.rename(
            os.path.join(olapy_dir, "olapy-config.yml"),
            os.path.join(olapy_dir, "backup_olapy-config.yml"),
        )


def restore_config_file():
    olapy_dir = get_olapy_dir()
    if os.path.isfile(os.path.join(olapy_dir, "backup_olapy-config.yml")):
        os.rename(
            os.path.join(olapy_dir, "backup_olapy-config.yml"),
            os.path.join(olapy_dir, "olapy-config.yml"),
        )
        os.remove(os.path.join(olapy_dir, "backup_olapy-config.yml"))


def main():
    file = open(
        "DATABASES_bench_result"
        + str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")),
        "w",
    )

    file.write("Benchmarks are made with cpu :\n")
    file.write(cpuinfo.get_cpu_info()["brand"] + "\n\n")

    for idx, query in enumerate([query1, query6, query7, query9]):
        file.write(
            "Query {0} :\n".format(str(idx + 1))
            + query
            + "\n----------------------------------------------------------\n\n"
        )

    sqlalchemy_uri = os.environ.get("SQLALCHEMY_DATABASE_URI", "sqlite://")
    db = urlparse(sqlalchemy_uri).path.replace("/", "")
    engine = sqlalchemy.create_engine(sqlalchemy_uri)
    mdx_engine = MdxEngine(sqla_engine=engine, source_type="db")
    mdx_engine.load_cube(cube_name=db if db else "main", fact_table_name="facts")
    wsgi_application = get_wsgi_application(mdx_engine)

    server = WSGIServer(application=wsgi_application, host=HOST, port=PORT)

    server.start()

    for config_file in os.listdir(os.path.join(os.getcwd(), "db_config_files")):

        dbms = str(config_file.split("_")[0])

        try:
            copy_2_olapy_dir(config_file)

            # to refresh cubes from database

            provider = xmla.XMLAProvider()
            conn = provider.connect(location=server.url)

            mbench = MicBench()
            t = PrettyTable(["Query", "{0} - olapy execution time".format(dbms)])

            for idx, query in enumerate([query1, query6, query7, query9]):

                if dbms.upper() in ["POSTGRES", "ORACLE"]:
                    query = fix_query_lowercase_db(query)
                t.add_row(
                    ["Query" + str(idx + 1), str(mbench.bench(conn, query, CUBE_NAME))]
                )

            file.write(str(t) + "\n\n")
        except XMLAException:
            type, value, traceback = sys.exc_info()
            print("Error opening %s" % (value))
            print("Can't connect to {0} database".format(dbms))
    server.stop()


if __name__ == "__main__":
    main()
