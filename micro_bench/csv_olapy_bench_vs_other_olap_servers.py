from __future__ import absolute_import, division, print_function, \
    unicode_literals

import cProfile
import datetime
import os
import pstats
from os.path import expanduser

import cpuinfo
from olap.xmla import xmla
from prettytable import PrettyTable
from spyne.server.wsgi import WsgiApplication
from tests.test_xmla import WSGIServer

from olapy.core.mdx.executor import MdxEngine
# do not remove this (used in profiler)
from olapy.core.services.models import Command, ExecuteRequest, \
    Propertieslist, Property
from olapy.core.services.xmla import XmlaProviderService, get_spyne_app
from olapy.core.services import XmlaDiscoverReqHandler

from .cube_generator import CUBE_NAME, CubeGen
from .micro_bench import MicBench

HOST = "127.0.0.1"
PORT = 8230
REFINEMENT_LVL = 5
PROFILING_LINES = 15


class Config(object):

    def __init__(self, xmla_tools):
        self.config = {"xmla_tools": xmla_tools}


def olapy_vs_mondrian(file, mbench, conn):
    try:
        file.write(
            '******************************************************************************\n'
        )
        file.write(
            '* mondrian with "warehouse" Cube (note the same as olapy but resemble to it) *\n'
        )
        file.write(
            '* (olapy warehouse"s cube has more rows)                                     *\n'
        )
        file.write(
            '******************************************************************************\n\n'
        )

        t = PrettyTable(['Query', 'mondrian', 'olapy'])
        p2 = xmla.XMLAProvider()
        c2 = p2.connect(location="http://localhost:8080/xmondrian/xmla")

        cmd = """SELECT
               FROM [foodmart]
               WHERE ([Measures].[supply_time])
               CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        cmd2 = """SELECT
               NON EMPTY {[Measures].[Supply Time]}
               DIMENSION PROPERTIES PARENT_UNIQUE_NAME ON 0
               FROM
               [Warehouse]"""

        file.write(
            "Query 1 :\n" + cmd2 +
            "\n----------------------------------------------------------\n\n")
        t.add_row([
            'Query 1',
            mbench.bench(c2, cmd2, 'FoodMart'),
            mbench.bench(conn, cmd, 'foodmart')
        ])

        cmd = """SELECT NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember({{{
          [Product].[Product].[All brand_name].Members}}}, {
          [Product].[Product].[brand_name].[Pearl]})))
          DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
          FROM [foodmart]
          WHERE ([Measures].[supply_time])
          CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        cmd2 = """SELECT
            NON EMPTY CrossJoin(Hierarchize({
            [Product].[Brand Name].Members,
            [Product].[Drink].[Alcoholic Beverages].[Beer and Wine].[Wine].[Pearl].Children}), {
            [Measures].[Supply Time]})
            DIMENSION PROPERTIES PARENT_UNIQUE_NAME ON 0
            FROM [Warehouse]"""

        file.write(
            "Query 2 :\n" + cmd2 +
            "\n----------------------------------------------------------\n\n")
        t.add_row([
            'Query 2',
            mbench.bench(c2, cmd2, 'FoodMart'),
            mbench.bench(conn, cmd, 'foodmart')
        ])

        cmd = """SELECT NON EMPTY CrossJoin(Hierarchize(AddCalculatedMembers({
          [Product].[Product].[All brand_name].Members})), Hierarchize(AddCalculatedMembers({
          [Store].[Store].[All store_type].Members})))
          DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
          FROM [foodmart]
          WHERE ([Measures].[supply_time])
          CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        cmd2 = """SELECT
            NON EMPTY CrossJoin(CrossJoin(Hierarchize({
            [Product].[Brand Name].Members}),Hierarchize({
            [Store Type].[All Store Types],
            [Store Type].[All Store Types].Children})),
            {[Measures].[Supply Time]})
            DIMENSION PROPERTIES PARENT_UNIQUE_NAME ON 0
            FROM [Warehouse]"""

        file.write(
            "Query 3 :\n" + cmd2 +
            "\n----------------------------------------------------------\n\n")
        t.add_row([
            'Query 3',
            mbench.bench(c2, cmd2, 'FoodMart'),
            mbench.bench(conn, cmd, 'foodmart')
        ])

        file.write(str(t) + "\n\n")

    except:
        print('Make sure mondrian is running and containing Warehouse cube')
        pass


def olapy_vs_iccube(file, mbench, conn):
    try:
        file.write('******************************************\n')
        file.write('* iCcube v4.8.2 with "sales Excel" Cube  *\n')
        file.write('******************************************\n\n')

        t = PrettyTable(['Query', 'olapy', 'icCube'])
        p2 = xmla.XMLAProvider()
        c2 = p2.connect(
            location="http://localhost:8282/icCube/xmla",
            username="demo",
            password="demo")

        cmd = """SELECT
          FROM [Sales]
          WHERE ([Measures].[Amount])
          CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        file.write(
            "Query 1 :\n" + cmd +
            "\n----------------------------------------------------------\n\n")
        t.add_row([
            'Query 1',
            mbench.bench(conn, cmd, 'sales'),
            mbench.bench(c2, cmd, 'Sales (Excel)')
        ])

        cmd = """SELECT
                NON EMPTY Hierarchize(AddCalculatedMembers({DrilldownLevel({
                [Geography].[Geo].[All Regions]})}))
                DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
                FROM [Sales]
                WHERE ([Measures].[Amount])
                CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        cmd2 = """SELECT
                  NON EMPTY Hierarchize(AddCalculatedMembers({DrilldownLevel({
                  [Geography].[Geo].[All Continent]})}))
                  DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
                  ON COLUMNS
                  FROM [Sales]
                  WHERE ([Measures].[Amount])
                  CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""
        file.write(
            "Query 2 :\n" + cmd2 +
            "\n----------------------------------------------------------\n\n")
        t.add_row([
            'Query 2',
            mbench.bench(conn, cmd2, 'sales'),
            mbench.bench(c2, cmd, 'Sales (Excel)')
        ])

        cmd = """SELECT
                 NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{DrilldownLevel({
                 [Geography].[Geo].[All Regions]})}}, {
                 [Geography].[Geo].[All Regions].&amp;[America],
                 [Geography].[Geo].[All Regions].&amp;[Europe]})}}, {
                 [Geography].[Geo].[All Regions].&amp;[America].&amp;[US],
                 [Geography].[Geo].[All Regions].&amp;[Europe].&amp;[FR],
                 [Geography].[Geo].[All Regions].&amp;[Europe].&amp;[ES]})))
                 DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
                 ON COLUMNS
                 FROM [Sales]
                 WHERE ([Measures].[Amount])
                 CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        cmd2 = """SELECT
                  NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{{
                  [Geography].[Geography].[All Continent].Members}}}, {
                  [Geography].[Geography].[Continent].[America],
                  [Geography].[Geography].[Continent].[Europe]})}}, {
                  [Geography].[Geography].[Continent].[America].[United States],
                  [Geography].[Geography].[Continent].[Europe].[France],
                  [Geography].[Geography].[Continent].[Europe].[Spain]})))
                  DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
                  ON COLUMNS
                  FROM [sales]
                  WHERE ([Measures].[Amount])
                  CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""
        file.write(
            "Query 3 :\n" + cmd2 +
            "\n----------------------------------------------------------\n\n")
        t.add_row([
            'Query 3',
            mbench.bench(conn, cmd2, 'sales'),
            mbench.bench(c2, cmd, 'Sales (Excel)')
        ])

        cmd = """SELECT
                 NON EMPTY CrossJoin(Hierarchize(AddCalculatedMembers(DrilldownMember
                 ({{DrilldownMember({{DrilldownLevel({
                 [Geography].[Geo].[All Regions]})}}, {
                 [Geography].[Geo].[All Regions].&amp;[America],
                 [Geography].[Geo].[All Regions].&amp;[Europe]})}}, {
                 [Geography].[Geo].[All Regions].&amp;[America].&amp;[US],
                 [Geography].[Geo].[All Regions].&amp;[Europe].&amp;[FR],
                 [Geography].[Geo].[All Regions].&amp;[Europe].&amp;[ES]}))), Hierarchize(AddCalculatedMembers({
                 [Product].[Prod].[Company].Members}))) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
                 ON COLUMNS
                 FROM [Sales]
                 WHERE ([Measures].[Amount])
                 CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        cmd2 = """SELECT
                  NON EMPTY CrossJoin(Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{{
                  [Geography].[Geography].[All Continent].Members}}}, {
                  [Geography].[Geography].[Continent].[America],
                  [Geography].[Geography].[Continent].[Europe]})}}, {
                  [Geography].[Geography].[Continent].[America].[United States],
                  [Geography].[Geography].[Continent].[Europe].[France],
                  [Geography].[Geography].[Continent].[Europe].[Spain]}))), Hierarchize(AddCalculatedMembers({
                  [Product].[Product].[Company].Members})))
                  DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
                  ON COLUMNS
                  FROM [sales]
                  WHERE ([Measures].[Amount])
                  CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        file.write(
            "Query 4 :\n" + cmd2 +
            "\n----------------------------------------------------------\n\n")
        t.add_row([
            'Query 4',
            mbench.bench(conn, cmd2, 'sales'),
            mbench.bench(c2, cmd, 'Sales (Excel)')
        ])

        cmd = """SELECT
                 NON EMPTY CrossJoin(CrossJoin(Hierarchize(AddCalculatedMembers
                 (DrilldownMember({{DrilldownMember({{DrilldownLevel({
                 [Geography].[Geo].[All Regions]})}}, {
                 [Geography].[Geo].[All Regions].&amp;[America],
                 [Geography].[Geo].[All Regions].&amp;[Europe]})}}, {
                 [Geography].[Geo].[All Regions].&amp;[America].&amp;[US],
                 [Geography].[Geo].[All Regions].&amp;[Europe].&amp;[FR],
                 [Geography].[Geo].[All Regions].&amp;[Europe].&amp;[ES]}))),
                 Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{
                 [Product].[Prod].[Company].Members}}, {
                 [Product].[Prod].[Company].&amp;[Crazy Development ]})}}, {
                 [Product].[Prod].[Company].&amp;[Crazy Development ].&amp;[icCube]})))),
                 Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{DrilldownMember({{
                 [Time].[Calendar].[Year].Members}}, {
                 [Time].[Calendar].[Year].&amp;[2010]})}}, {
                 [Time].[Calendar].[Year].&amp;[2010].&amp;[Q2 2010]})}}, {
                 [Time].[Calendar].[Year].&amp;[2010].&amp;[Q2 2010].&amp;[May 2010]}))))
                 DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
                 ON COLUMNS
                 FROM [Sales]
                 WHERE ([Measures].[Amount])
                 CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        cmd2 = """SELECT
                  NON EMPTY CrossJoin(CrossJoin(Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{{
                  [Geography].[Geography].[All Continent].Members}}}, {
                  [Geography].[Geography].[Continent].[America],
                  [Geography].[Geography].[Continent].[Europe]})}}, {
                  [Geography].[Geography].[Continent].[America].[United States],
                  [Geography].[Geography].[Continent].[Europe].[France],
                  [Geography].[Geography].[Continent].[Europe].[Spain]}))),
                  Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{
                  [Product].[Product].[Company].Members}}, {
                  [Product].[Product].[Company].[Crazy Development]})}}, {
                  [Product].[Product].[Company].[Crazy Development].[olapy]})))),
                  Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{DrilldownMember({{
                  [Time].[Time].[Year].Members}}, {
                  [Time].[Time].[Year].[2010]})}}, {
                  [Time].[Time].[Year].[2010].[Q2 2010]})}}, {
                  [Time].[Time].[Year].[2010].[Q2 2010].[May 2010]}))))
                  DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
                  ON COLUMNS
                  FROM [sales]
                  WHERE ([Measures].[Amount])
                  CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""

        file.write(
            "Query 5 :\n" + cmd2 +
            "\n----------------------------------------------------------\n\n")
        t.add_row([
            'Query 5',
            mbench.bench(conn, cmd2, 'sales'),
            mbench.bench(c2, cmd, 'Sales (Excel)')
        ])

        file.write(str(t) + "\n\n")

    except:
        print('Make sure icCube is running and containing sales Excel cube')
        pass


def olapy_query_execution_bench(file, mbench, conn, xmla_tools):
    t = PrettyTable(['Query', 'olapy execution time'])
    cmd = """SELECT FROM [""" + CUBE_NAME + """] WHERE ([Measures].[Amount])
            CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS"""
    file.write(
        "Query 1 :\n" + cmd +
        "\n----------------------------------------------------------\n\n")
    t.add_row(['Query 1', mbench.bench(conn, cmd, CUBE_NAME)])
    cmd = """SELECT NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember({{{
        [table0].[table0].[All table0A].Members}}}, {
        [table0].[table0].[table0A].[""" + str(
        xmla_tools.executor.star_schema_dataframe.table0A[1]) + """]})))
        DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
        ON COLUMNS FROM [""" + CUBE_NAME + """] WHERE ([Measures].[Amount])"""
    file.write(
        "Query 2 :\n" + cmd +
        "\n----------------------------------------------------------\n\n")
    t.add_row(['Query 2', mbench.bench(conn, cmd, CUBE_NAME)])
    tup = "[table0].[table0].[table0A].[" + str(
        xmla_tools.executor.star_schema_dataframe.table0A[0]) + "]"
    for d in range(REFINEMENT_LVL):
        tup += ",\n[table0].[table0].[table0A].[" + str(
            xmla_tools.executor.star_schema_dataframe.table0A[d + 1]) + "]"
    cmd = """SELECT NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember({{{
        [table0].[table0].[All table0A].Members}}}, {""" + tup + """})))
        DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
        ON COLUMNS FROM [""" + CUBE_NAME + """] WHERE ([Measures].[Amount])"""
    file.write(
        "Query 3 :\n" + cmd +
        "\n----------------------------------------------------------\n\n")
    t.add_row(['Query 3', mbench.bench(conn, cmd, CUBE_NAME)])
    file.write(str(t) + "\n\n")


def olapy_profile(file):
    file.write(
        '---------------- Profiling olapy Query 5 ------------------ \n\n')
    cProfile.run("""cmd = '''
                SELECT
                NON EMPTY CrossJoin(CrossJoin(Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{{
                [Geography].[Geography].[All Continent].Members}}}, {
                [Geography].[Geography].[Continent].[America],
                [Geography].[Geography].[Continent].[Europe]})}}, {
                [Geography].[Geography].[Continent].[America].[United States],
                [Geography].[Geography].[Continent].[Europe].[France],
                [Geography].[Geography].[Continent].[Europe].[Spain]}))),
                Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{
                [Product].[Product].[Company].Members}}, {
                [Product].[Product].[Company].[Crazy Development]})}}, {
                [Product].[Product].[Company].[Crazy Development].[olapy]})))),
                Hierarchize(AddCalculatedMembers(DrilldownMember({{DrilldownMember({{DrilldownMember({{
                [Time].[Time].[Year].Members}}, {
                [Time].[Time].[Year].[2010]})}}, {
                [Time].[Time].[Year].[2010].[Q2 2010]})}}, {
                [Time].[Time].[Year].[2010].[Q2 2010].[May 2010]}))))
                DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
                ON COLUMNS
                FROM [sales]
                WHERE ([Measures].[Amount])
                CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS'''

request = ExecuteRequest()
request.Command = Command(Statement=cmd)
request.Properties = Propertieslist(PropertyList=Property(Catalog='sales'))
olapy_data = os.path.join(expanduser('~'), 'olapy-data')
xmla_tools = XmlaDiscoverReqHandler(olapy_data=olapy_data, source_type='csv',
                       db_config=None, cubes_config=None)

xmla_p_server = XmlaProviderService()
setattr(xmla_p_server,"app", Config(xmla_tools))
xmla_p_server.Execute(xmla_p_server, request)
""", "{}.profile".format(__file__))

    s = pstats.Stats("{}.profile".format(__file__), stream=file)
    s.strip_dirs()
    s.sort_stats("time").print_stats(PROFILING_LINES)

    try:
        os.system(
            'gprof2dot -f pstats csv_olapy_bench_vs_other_olap_servers.py.profile | dot -Tpng -o profile.png'
        )
    except:
        print('make sure gprof2dot and graphviz are installed')

    os.remove('csv_olapy_bench_vs_other_olap_servers.py.profile')


def _get_xmla_tools():
    olapy_data = os.path.join(expanduser('~'), 'olapy-data')
    mdx_executor = MdxEngine()
    mdx_executor.load_cube(CUBE_NAME)
    xmla_tools = XmlaDiscoverReqHandler(
        executor=mdx_executor,
        olapy_data=olapy_data,
        source_type='csv',
        db_config=None,
        cubes_config=None)
    return xmla_tools


def main():
    file = open('bench_result' +
                str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")), 'w')
    gen = CubeGen(number_dimensions=3, rows_length=1000, columns_length=5)
    gen.generate_csv(gen.generate_cube(3, 1000))
    mbench = MicBench()
    file.write("Benchmarks are made with cpu :\n")
    file.write(cpuinfo.get_cpu_info()['brand'] + "\n\n")
    xmla_tools = _get_xmla_tools()
    application = get_spyne_app(xmla_tools)
    wsgi_application = WsgiApplication(application)
    server = WSGIServer(application=wsgi_application, host=HOST, port=PORT)
    server.start()
    provider = xmla.XMLAProvider()
    conn = provider.connect(location=server.url)
    olapy_query_execution_bench(file, mbench, conn, xmla_tools)
    olapy_vs_mondrian(file, mbench, conn)
    olapy_vs_iccube(file, mbench, conn)
    olapy_profile(file)
    gen.remove_temp_cube()
    file.close()
    server.stop()


if __name__ == '__main__':
    main()
