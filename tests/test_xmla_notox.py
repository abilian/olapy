from __future__ import absolute_import, division, print_function

import os  # Don't remove !!!
import threading

import pytest
from olap.xmla import xmla
from spyne import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from werkzeug.serving import make_server

from olapy.core.services.xmla import XmlaProviderService
from tests.xs0_responses import TEST_QUERY_AXIS0

HOST = "127.0.0.1"
PORT = 8230
RUNNING_TOX = 'RUNTING_TOX' in os.environ


class Member(object):

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))


class WSGIServer:
    """
    HTTP server running a WSGI application in its own thread.

    Copy/pasted from pytest_localserver w/ slight changes.
    """

    def __init__(self, host='127.0.0.1', port=8000, application=None, **kwargs):
        self._server = make_server(host, port, application, **kwargs)
        self.server_address = self._server.server_address

        self.thread = threading.Thread(
            name=self.__class__, target=self._server.serve_forever)

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
        proto = 'http' if self._server.ssl_context is None else 'https'
        return '{0}://{1}:{2}'.format(proto, host, port)


@pytest.fixture(scope="function")
def conn():
    print("spawning server")
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
    yield conn

    print("stopping server")
    server.stop()


@pytest.mark.skipif("RUNNING_TOX")
def test_connection(conn):
    assert len(conn.getCatalogs()) > 0


@pytest.mark.skipif("RUNNING_TOX")
def test_DISCOVER_PROPERTIES(conn):
    discover = conn.Discover(
        'DISCOVER_PROPERTIES',
        properties={'LocaleIdentifier': '1036'},
        restrictions={'PropertyName': 'Catalog'})[0]
    assert discover['PropertyName'] == "Catalog"
    assert discover['PropertyDescription'] == "Catalog"
    assert discover['PropertyType'] == "string"
    assert discover['PropertyAccessType'] == "ReadWrite"
    assert discover['IsRequired'] == "false"
    assert discover['Value'] == "olapy Unspecified Catalog"


@pytest.mark.skipif("RUNNING_TOX")
def test_MDSCHEMA_CUBES(conn):
    discover = conn.Discover(
        "MDSCHEMA_CUBES",
        restrictions={'CUBE_NAME': 'sales'},
        properties={'Catalog': 'sales'})[0]

    assert discover['CATALOG_NAME'] == "sales"
    assert discover['CUBE_NAME'] == "sales"
    assert discover['CUBE_TYPE'] == "CUBE"
    assert discover['IS_DRILLTHROUGH_ENABLED'] == "true"
    assert discover['CUBE_CAPTION'] == "sales"


@pytest.mark.skipif("RUNNING_TOX")
def test_query1(conn):
    # only one measure selected
    # Result :

    # Amount
    # 1023

    cmd = """
    SELECT
    FROM [sales]
    WHERE ([Measures].[Amount])
     CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    res = conn.Execute(cmd, Catalog="sales")
    assert res.cellmap[0]['_CellOrdinal'] == '0'
    assert res.cellmap[0]['Value'] == 1023


@pytest.mark.skipif("RUNNING_TOX")
def test_query2(conn):
    # drill down on one Dimension
    # Result :

    # Row Labels	       Amount
    # All Continent	        1023
    # America	            768
    # United States	    768
    # New York	    768
    # Europe	            255
    # France	        4
    # Spain	            3
    # Barcelona	    2
    # Madrid	    1
    # Switzerland	    248

    # This kind of query is generated by excel once you select a dimension a you do drill dow
    cmd = """
    SELECT
    NON EMPTY Hierarchize(AddCalculatedMembers(DrilldownMember(
    {{DrilldownMember({{{[Geography].[Geography].[All Continent].Members}}},
    {[Geography].[Geography].[Continent].[America],
    [Geography].[Geography].[Continent].[Europe]})}},
    {[Geography].[Geography].[Continent].[America].[United States],
    [Geography].[Geography].[Continent].[Europe].[Spain]})))
    DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
    ON COLUMNS
    FROM [sales]
    WHERE ([Measures].[Amount])
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    res = conn.Execute(cmd, Catalog="sales")
    columns = []
    values = []
    for cell in res.cellmap.viewitems():
        columns.append(res.getAxisTuple('Axis0')[cell[0]])
        values.append(cell[1]['Value'])
    mems = []
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[Continent].[America]',
            Caption='America',
            LName='[Geography].[Geography].[Continent]',
            LNum='0',
            DisplayInfo='131076',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[Country].[America].[United States]',
            Caption='United States',
            LName='[Geography].[Geography].[Country]',
            LNum='1',
            DisplayInfo='131076',
            PARENT_UNIQUE_NAME='[Geography].[Geography].[Continent].[America]',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[City].[America].[United States].[New York]',
            Caption='New York',
            LName='[Geography].[Geography].[City]',
            LNum='2',
            DisplayInfo='131076',
            PARENT_UNIQUE_NAME='[Geography].[Geography].[Continent].[America].[United States]',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[Continent].[Europe]',
            Caption='Europe',
            LName='[Geography].[Geography].[Continent]',
            LNum='0',
            DisplayInfo='131076',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[Country].[Europe].[France]',
            Caption='France',
            LName='[Geography].[Geography].[Country]',
            LNum='1',
            DisplayInfo='131076',
            PARENT_UNIQUE_NAME='[Geography].[Geography].[Continent].[Europe]',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[Country].[Europe].[Spain]',
            Caption='Spain',
            LName='[Geography].[Geography].[Country]',
            LNum='1',
            DisplayInfo='131076',
            PARENT_UNIQUE_NAME='[Geography].[Geography].[Continent].[Europe]',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[City].[Europe].[Spain].[Barcelona]',
            Caption='Barcelona',
            LName='[Geography].[Geography].[City]',
            LNum='2',
            DisplayInfo='131076',
            PARENT_UNIQUE_NAME='[Geography].[Geography].[Continent].[Europe].[Spain]',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[City].[Europe].[Spain].[Madrid]',
            Caption='Madrid',
            LName='[Geography].[Geography].[City]',
            LNum='2',
            DisplayInfo='131076',
            PARENT_UNIQUE_NAME='[Geography].[Geography].[Continent].[Europe].[Spain]',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    mems.append(
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[Country].[Europe].[Switzerland]',
            Caption='Switzerland',
            LName='[Geography].[Geography].[Country]',
            LNum='1',
            DisplayInfo='131076',
            PARENT_UNIQUE_NAME='[Geography].[Geography].[Continent].[Europe]',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'))
    assert [Member(**dict(co)) for co in columns] == mems
    assert values == [768, 768, 768, 255, 4, 3, 2, 1, 248]


@pytest.mark.skipif("RUNNING_TOX")
def test_query3(conn):
    # Many Dimensions selected
    # Result :

    # Row Labels               Amount
    # All Continent
    # America
    #     Crazy Development
    #         2010
    #                           768
    # Europe
    #     Crazy Development
    #         2010
    #                           255

    # This kind of query is generated by excel once you select a dimension a you do drill dow
    cmd = """
        SELECT NON EMPTY
        CrossJoin(CrossJoin(Hierarchize(AddCalculatedMembers({
        [Geography].[Geography].[All Continent].Members})),
        Hierarchize(AddCalculatedMembers({
        [Product].[Product].[Company].Members}))),
        Hierarchize(AddCalculatedMembers({[Time].[Time].[Year].Members})))
        DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
        FROM [sales]
        WHERE ([Measures].[Amount])
        CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
        """
    res = conn.Execute(cmd, Catalog="sales")
    columns = []
    values = []
    for cell in res.cellmap.viewitems():
        columns.append(res.getAxisTuple('Axis0')[cell[0]])
        values.append(cell[1]['Value'])

    mems = []
    mems.append([
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[Continent].[America]',
            Caption='America',
            LName='[Geography].[Geography].[Continent]',
            LNum='0',
            DisplayInfo='131076',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'), Member(
                _Hierarchy='[Product].[Product]',
                UName='[Product].[Product].[Company].[Crazy Development]',
                Caption='Crazy Development',
                LName='[Product].[Product].[Company]',
                LNum='0',
                DisplayInfo='131076',
                HIERARCHY_UNIQUE_NAME='[Product].[Product]'), Member(
                    _Hierarchy='[Time].[Time]',
                    UName='[Time].[Time].[Year].[2010]',
                    Caption='2010',
                    LName='[Time].[Time].[Year]',
                    LNum='0',
                    DisplayInfo='131076',
                    HIERARCHY_UNIQUE_NAME='[Time].[Time]')
    ])
    mems.append([
        Member(
            _Hierarchy='[Geography].[Geography]',
            UName='[Geography].[Geography].[Continent].[Europe]',
            Caption='Europe',
            LName='[Geography].[Geography].[Continent]',
            LNum='0',
            DisplayInfo='131076',
            HIERARCHY_UNIQUE_NAME='[Geography].[Geography]'), Member(
                _Hierarchy='[Product].[Product]',
                UName='[Product].[Product].[Company].[Crazy Development]',
                Caption='Crazy Development',
                LName='[Product].[Product].[Company]',
                LNum='0',
                DisplayInfo='131076',
                HIERARCHY_UNIQUE_NAME='[Product].[Product]'), Member(
                    _Hierarchy='[Time].[Time]',
                    UName='[Time].[Time].[Year].[2010]',
                    Caption='2010',
                    LName='[Time].[Time].[Year]',
                    LNum='0',
                    DisplayInfo='131076',
                    HIERARCHY_UNIQUE_NAME='[Time].[Time]')
    ])

    for idx, item in enumerate(columns):
        assert [Member(**dict(co)) for co in item] == mems[idx]
    assert values == [768, 255]


@pytest.mark.skipif("RUNNING_TOX")
def test_query4(conn):
    # Many Dimensions selected with different measures
    # Result :

    # Row Labels
    # Amount
    #     Crazy Development
    #         olapy
    #             All Continent
    #                 America
    #                     2010        768
    #             Europe
    #                 France
    #                     2010        4
    #                 Spain
    #                     2010        3
    #                 Switzerland
    #                     2010        248
    # Count
    #     Crazy Development
    #         olapy
    #             All Continent
    #                 America
    #                     2010        576
    #             Europe
    #                 France
    #                     2010        2
    #             Spain
    #                     2010        925
    #             Switzerland
    #                     2010        377

    # This kind of query is generated by excel once you select a dimension a you do drill dow
    cmd = """
    SELECT
    NON EMPTY CrossJoin(CrossJoin(CrossJoin({
    [Measures].[Amount],
    [Measures].[Count]},
    Hierarchize(AddCalculatedMembers(DrilldownMember({{
    [Product].[Product].[Company].Members}}, {
    [Product].[Product].[Company].[Crazy Development]
    })))), Hierarchize(AddCalculatedMembers(DrilldownMember({{{
    [Geography].[Geography].[All Continent].Members}}}, {
    [Geography].[Geography].[Continent].[Europe]})))), Hierarchize(AddCalculatedMembers({
    [Time].[Time].[Year].Members}))) DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
    ON COLUMNS
    FROM [sales]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    res = conn.Execute(cmd, Catalog="sales")
    columns = []
    values = []
    for cell in res.cellmap.viewitems():
        columns.append(res.getAxisTuple('Axis0')[cell[0]])
        values.append(cell[1]['Value'])

    strr = """"""
    for idx, item in enumerate(columns):
        strr += str(item)
    assert strr == TEST_QUERY_AXIS0
    assert values == [
        768, 255, 4, 3, 248, 768, 255, 4, 3, 248, 576, 1304, 2, 925, 377, 576,
        1304, 2, 925, 377
    ]
