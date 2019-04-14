from __future__ import absolute_import, division, print_function, \
    unicode_literals

import threading
from wsgiref.simple_server import make_server

import pytest
import sqlalchemy
from olap.xmla import xmla
from spyne import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

from olapy.core.mdx.executor import MdxEngine
from olapy.core.services import XmlaDiscoverReqHandler, XmlaExecuteReqHandler
from olapy.core.services.xmla import XmlaProviderService

from .db_creation_utils import create_insert, drop_tables
from .xs0_responses import TEST_QUERY_AXIS0

# pytest.importorskip("sqlalchemy")
# pytest.importorskip("spyne")

HOST = "127.0.0.1"
PORT = 8230


class Member(object):
    "Encapsulates xs0 response attributes."

    def __init__(self, **kwargs):
        """
        :param kwargs: [_Hierarchy,UName,Caption,LName,LNum,DisplayInfo,
            PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME]
        """
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

    def __repr__(self):
        return str(self.__dict__)


class WSGIServer:
    """HTTP server running a WSGI application in its own thread.

    Copy/pasted from pytest_localserver w/ slight changes.
    """

    def __init__(self, host="127.0.0.1", port=8000, application=None, **kwargs):
        self._server = make_server(host, port, application, **kwargs)
        self.server_address = self._server.server_address

        self.thread = threading.Thread(
            name=self.__class__, target=self._server.serve_forever
        )

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
        proto = "http"  # if self._server.ssl_context is None else 'https'
        return "{}://{}:{}".format(proto, host, port)


@pytest.fixture(scope="module")
def conn():
    engine = sqlalchemy.create_engine("sqlite://")
    create_insert(engine)
    executor = MdxEngine(sqla_engine=engine, source_type="db")
    executor.load_cube(cube_name="main", fact_table_name="facts")
    discover_request_hanlder = XmlaDiscoverReqHandler(executor)
    execute_request_hanlder = XmlaExecuteReqHandler(executor)

    print("spawning server")
    application = Application(
        [XmlaProviderService],
        "urn:schemas-microsoft-com:xml-analysis",
        in_protocol=Soap11(validator="soft"),
        out_protocol=Soap11(validator="soft"),
        config={
            "discover_request_hanlder": discover_request_hanlder,
            "execute_request_hanlder": execute_request_hanlder,
        },
    )

    wsgi_application = WsgiApplication(application)
    server = WSGIServer(application=wsgi_application, host=HOST, port=PORT)

    server.start()

    provider = xmla.XMLAProvider()
    yield provider.connect(location=server.url)

    print("stopping server")
    drop_tables(engine)
    server.stop()


def test_connection(conn):
    assert len(conn.getCatalogs()) > 0


def test_discover_properties(conn):
    discover = conn.Discover(
        "DISCOVER_PROPERTIES",
        properties={"LocaleIdentifier": "1036"},
        restrictions={"PropertyName": "Catalog"},
    )[0]
    assert discover["PropertyName"] == "Catalog"
    assert discover["PropertyDescription"] == "Catalog"
    assert discover["PropertyType"] == "string"
    assert discover["PropertyAccessType"] == "ReadWrite"
    assert discover["IsRequired"] == "false"
    # assert discover['Value'] == "olapy Unspecified Catalog" not necessary


def test_mdschema_cubes(conn):
    discover = conn.Discover(
        "MDSCHEMA_CUBES",
        restrictions={"CUBE_NAME": "main"},
        properties={"Catalog": "main"},
    )[0]
    assert discover["CATALOG_NAME"] == "main"
    assert discover["CUBE_NAME"] == "main"
    assert discover["CUBE_TYPE"] == "CUBE"
    assert discover["IS_DRILLTHROUGH_ENABLED"] == "true"
    assert discover["CUBE_CAPTION"] == "main"


def test_query1(conn):
    # only one measure selected
    # Result :

    # amount
    # 1023
    cmd = """
    SELECT
    FROM [main]
    WHERE ([Measures].[amount])
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """

    res = conn.Execute(cmd, Catalog="main")
    assert res.cellmap[0]["_CellOrdinal"] == "0"
    assert res.cellmap[0]["Value"] == 1023


def test_query2(conn):
    # drill down on one Dimension
    # Result :

    # Row Labels	       amount
    # All continent	        1023
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
        {{DrilldownMember({{{[geography].[geography].[All continent].Members}}},
        {[geography].[geography].[continent].[America],
        [geography].[geography].[continent].[Europe]})}},
        {[geography].[geography].[continent].[America].[United States],
        [geography].[geography].[continent].[Europe].[Spain]})))
    DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
    ON COLUMNS
    FROM [main]
    WHERE ([Measures].[amount])
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    res = conn.Execute(cmd, Catalog="main")
    columns = []
    values = []
    for cell in res.cellmap.items():
        columns.append(res.getAxisTuple("Axis0")[cell[0]])
        values.append(cell[1]["Value"])
    assert values == [768, 768, 768, 255, 4, 3, 2, 1, 248]

    expected = []
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[continent].[America]",
            Caption="America",
            LName="[geography].[geography].[continent]",
            LNum="0",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[country].[America].[United States]",
            Caption="United States",
            LName="[geography].[geography].[country]",
            LNum="1",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent].[America]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[city].[America].[United States].[New York]",
            Caption="New York",
            LName="[geography].[geography].[city]",
            LNum="2",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent].[America].[United States]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[continent].[Europe]",
            Caption="Europe",
            LName="[geography].[geography].[continent]",
            LNum="0",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[country].[Europe].[France]",
            Caption="France",
            LName="[geography].[geography].[country]",
            LNum="1",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent].[Europe]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[country].[Europe].[Spain]",
            Caption="Spain",
            LName="[geography].[geography].[country]",
            LNum="1",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent].[Europe]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[city].[Europe].[Spain].[Barcelona]",
            Caption="Barcelona",
            LName="[geography].[geography].[city]",
            LNum="2",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent].[Europe].[Spain]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[city].[Europe].[Spain].[Madrid]",
            Caption="Madrid",
            LName="[geography].[geography].[city]",
            LNum="2",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent].[Europe].[Spain]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    expected.append(
        Member(
            _Hierarchy="[geography].[geography]",
            UName="[geography].[geography].[country].[Europe].[Switzerland]",
            Caption="Switzerland",
            LName="[geography].[geography].[country]",
            LNum="1",
            DisplayInfo="131076",
            PARENT_UNIQUE_NAME="[geography].[geography].[continent].[Europe]",
            HIERARCHY_UNIQUE_NAME="[geography].[geography]",
        )
    )
    assert [Member(**dict(co)) for co in columns] == expected


def test_query3(conn):
    # Many Dimensions selected
    # Result :

    # Row Labels               amount
    # All continent
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
        [geography].[geography].[All continent].Members})),
        Hierarchize(AddCalculatedMembers({
        [product].[product].[company].Members}))),
        Hierarchize(AddCalculatedMembers({[time].[time].[year].Members})))
        DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME ON COLUMNS
        FROM [main]
        WHERE ([Measures].[amount])
        CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """
    res = conn.Execute(cmd, Catalog="main")
    columns = []
    values = []
    for cell in res.cellmap.items():
        columns.append(res.getAxisTuple("Axis0")[cell[0]])
        values.append(cell[1]["Value"])

    expected = []
    expected.append(
        [
            Member(
                _Hierarchy="[geography].[geography]",
                UName="[geography].[geography].[continent].[America]",
                Caption="America",
                LName="[geography].[geography].[continent]",
                LNum="0",
                DisplayInfo="131076",
                PARENT_UNIQUE_NAME="[geography].[geography].[continent]",
                HIERARCHY_UNIQUE_NAME="[geography].[geography]",
            ),
            Member(
                _Hierarchy="[product].[product]",
                UName="[product].[product].[company].[Crazy Development]",
                Caption="Crazy Development",
                LName="[product].[product].[company]",
                LNum="0",
                DisplayInfo="131076",
                PARENT_UNIQUE_NAME="[product].[product].[company]",
                HIERARCHY_UNIQUE_NAME="[product].[product]",
            ),
            Member(
                _Hierarchy="[time].[time]",
                UName="[time].[time].[year].[2010]",
                Caption="2010",
                LName="[time].[time].[year]",
                LNum="0",
                DisplayInfo="131076",
                PARENT_UNIQUE_NAME="[time].[time].[year]",
                HIERARCHY_UNIQUE_NAME="[time].[time]",
            ),
        ]
    )
    expected.append(
        [
            Member(
                _Hierarchy="[geography].[geography]",
                UName="[geography].[geography].[continent].[Europe]",
                Caption="Europe",
                LName="[geography].[geography].[continent]",
                LNum="0",
                DisplayInfo="131076",
                PARENT_UNIQUE_NAME="[geography].[geography].[continent]",
                HIERARCHY_UNIQUE_NAME="[geography].[geography]",
            ),
            Member(
                _Hierarchy="[product].[product]",
                UName="[product].[product].[company].[Crazy Development]",
                Caption="Crazy Development",
                LName="[product].[product].[company]",
                LNum="0",
                DisplayInfo="131076",
                PARENT_UNIQUE_NAME="[product].[product].[company]",
                HIERARCHY_UNIQUE_NAME="[product].[product]",
            ),
            Member(
                _Hierarchy="[time].[time]",
                UName="[time].[time].[year].[2010]",
                Caption="2010",
                LName="[time].[time].[year]",
                LNum="0",
                DisplayInfo="131076",
                PARENT_UNIQUE_NAME="[time].[time].[year]",
                HIERARCHY_UNIQUE_NAME="[time].[time]",
            ),
        ]
    )

    for idx, item in enumerate(columns):
        assert [Member(**dict(co)) for co in item] == expected[idx]
    assert values == [768, 255]


def test_query4(conn):
    # Many Dimensions selected with different measures
    # Result :

    # Row Labels
    # amount
    #     Crazy Development
    #         olapy
    #             All continent
    #                 America
    #                     2010        768
    #             Europe
    #                 France
    #                     2010        4
    #                 Spain
    #                     2010        3
    #                 Switzerland
    #                     2010        248
    # count
    #     Crazy Development
    #         olapy
    #             All continent
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
    SELECT NON EMPTY CrossJoin(CrossJoin(CrossJoin({
        [Measures].[amount],
        [Measures].[count]},
        Hierarchize(AddCalculatedMembers(DrilldownMember({{
        [product].[product].[company].Members}}, {
        [product].[product].[company].[Crazy Development]
        })))), Hierarchize(AddCalculatedMembers(DrilldownMember({{{
        [geography].[geography].[All continent].Members}}}, {
        [geography].[geography].[continent].[Europe]})))), Hierarchize(AddCalculatedMembers({
        [time].[time].[year].Members})))
        DIMENSION PROPERTIES PARENT_UNIQUE_NAME,HIERARCHY_UNIQUE_NAME
    ON COLUMNS
    FROM [main]
    CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS
    """

    res = conn.Execute(cmd, Catalog="main")
    columns = []
    values = []
    for cell in res.cellmap.items():
        columns.append(res.getAxisTuple("Axis0")[cell[0]])
        values.append(cell[1]["Value"])

    assert values == [
        768,
        255,
        4,
        3,
        248,
        768,
        255,
        4,
        3,
        248,
        576,
        1304,
        2,
        925,
        377,
        576,
        1304,
        2,
        925,
        377,
    ]

    strr = ""
    for item in columns:
        strr += str(item)
    assert strr == TEST_QUERY_AXIS0


def test_mdschema_members_MEMBER_UNIQUE_NAME(conn):
    discover = conn.Discover(
        "MDSCHEMA_MEMBERS",
        restrictions={
            "CUBE_NAME": "main",
            "MEMBER_UNIQUE_NAME": "[product].[product].[Crazy Development].[olapy].[Partnership]",
        },
        properties={"Catalog": "main"},
    )[0]
    assert discover["CATALOG_NAME"] == "main"
    assert discover["CUBE_NAME"] == "main"
    assert discover["DIMENSION_UNIQUE_NAME"] == "[product]"
    assert discover["HIERARCHY_UNIQUE_NAME"] == "[product].[product]"
    assert (
        discover["LEVEL_UNIQUE_NAME"]
        == "[product].[product].[Crazy Development].[olapy]"
    )
    assert discover["LEVEL_NUMBER"] == "3"
    assert discover["MEMBER_ORDINAL"] == "0"
    assert discover["MEMBER_NAME"] == "Partnership"
    assert (
        discover["MEMBER_UNIQUE_NAME"]
        == "[product].[product].[Crazy Development].[olapy].[Partnership]"
    )
    assert discover["MEMBER_TYPE"] == "1"
    assert discover["MEMBER_CAPTION"] == "Partnership"
    assert discover["CHILDREN_CARDINALITY"] == "1"
    assert discover["PARENT_LEVEL"] == "0"
    assert discover["PARENT_COUNT"] == "0"
    assert (
        discover["PARENT_UNIQUE_NAME"]
        == "[product].[product].[Crazy Development].[olapy]"
    )
    assert discover["MEMBER_KEY"] == "Partnership"
    assert discover["IS_PLACEHOLDERMEMBER"] == "false"
    assert discover["IS_DATAMEMBER"] == "false"


def test_mdschema_members_LEVEL_UNIQUE_NAME(conn):
    discover = conn.Discover(
        "MDSCHEMA_MEMBERS",
        restrictions={
            "CUBE_NAME": "main",
            "LEVEL_UNIQUE_NAME": "[product].[product].[Crazy Development].[olapy]",
        },
        properties={"Catalog": "main"},
    )[0]
    assert discover["CATALOG_NAME"] == "main"
    assert discover["CUBE_NAME"] == "main"
    assert discover["DIMENSION_UNIQUE_NAME"] == "[product]"
    assert discover["HIERARCHY_UNIQUE_NAME"] == "[product].[product]"
    assert discover["LEVEL_UNIQUE_NAME"] == "[product].[product].[Crazy Development]"
    assert discover["LEVEL_NUMBER"] == "2"
    assert discover["MEMBER_ORDINAL"] == "0"
    assert discover["MEMBER_NAME"] == "olapy"
    assert (
        discover["MEMBER_UNIQUE_NAME"]
        == "[product].[product].[Crazy Development].[olapy]"
    )
    assert discover["MEMBER_TYPE"] == "1"
    assert discover["MEMBER_CAPTION"] == "olapy"
    assert discover["CHILDREN_CARDINALITY"] == "1"
    assert discover["PARENT_LEVEL"] == "0"
    assert discover["PARENT_COUNT"] == "0"
    assert discover["PARENT_UNIQUE_NAME"] == "[product].[product].[Crazy Development]"
    assert discover["MEMBER_KEY"] == "olapy"
    assert discover["IS_PLACEHOLDERMEMBER"] == "false"
    assert discover["IS_DATAMEMBER"] == "false"
