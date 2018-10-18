# -*- encoding: utf8 -*-
"""
The main Module to manage `XMLA <https://technet.microsoft.com/fr-fr/library/ms187178(v=sql.90).aspx>`_ \
requests and responses, and managing Spyne soap server.

"""
from __future__ import absolute_import, division, print_function
# unicode_literals This is heavily discouraged with click

import imp
import logging
import os
from datetime import datetime
from os.path import expanduser
import xmlwitch
from .xmla_execute_tools import XmlaExecuteTools
from .xmla_execute_xsds import execute_xsd


class XmlaSoap11(Soap11):
    """XHR does not work over https without this patch"""

    def create_in_document(self, ctx, charset=None):
        if isinstance(ctx.transport, HttpTransportContext):
            http_verb = ctx.transport.get_request_method()
            if http_verb == "OPTIONS":
                ctx.transport.resp_headers["allow"] = "POST, OPTIONS"
                ctx.transport.respond(HTTP_200)
                raise Fault("")

        return Soap11.create_in_document(self, ctx, charset)


class XmlaProviderService(ServiceBase):
    """
    The main class to activate SOAP services between xmla clients and olapy.
    """

    def __init__(self, xmla_tools):
        self.xmla_tools = xmla_tools

    def Discover(self, request):
        """The first principle function of xmla protocol.

        :param request: :class:`DiscoverRequest` object

        :return: XML Discover response as string

        """
        # ctx is the 'context' parameter used by Spyne
        xmla_tools = ctx.app.config["xmla_tools"]
        ctx.out_header = Session(SessionId=str(xmla_tools.session_id))
        config_parser = xmla_tools.executor.cube_config
        if (config_parser and config_parser["xmla_authentication"] and
                ctx.transport.req_env["QUERY_STRING"] != "admin"):
            raise InvalidCredentialsError(
                fault_string="You do not have permission to access this resource",
                fault_object=None,
            )

        method_name = request.RequestType.lower() + "_response"
        method = getattr(discover_request_hanlder, method_name)

        if request.RequestType == "DISCOVER_DATASOURCES":
            return method()

        return method(request)

    def Execute(self, request):
        """The second principle function of xmla protocol.

        :param request: :class:`ExecuteRequest` object Execute.
        :return: XML Execute response as string
        """

        # same session_id in discover and execute
        ctx.out_header = Session(SessionId=str(ctx.app.config["discover_request_hanlder"].session_id))
        # same executor instance as the discovery (not reloading the cube another time)
        executor = ctx.app.config["discover_request_hanlder"].executor
        mdx_query = request.Command.Statement.encode().decode("utf8")
        if mdx_query == "":
            # check if command contains a query
            xml = xmlwitch.Builder()
            with xml["return"]:
                xml.root(xmlns="urn:schemas-microsoft-com:xml-analysis:empty")

            return str(xml)

        else:
            self.xmla_tools.change_catalogue(request.Properties.PropertyList.Catalog,
                                             )
            xml = xmlwitch.Builder()
            executor = self.xmla_tools.executor
        execute_request_hanlder = ctx.app.config["execute_request_hanlder"]

        # Hierarchize
        if all(key in mdx_query for key in ["WITH MEMBER", "strtomember", "[Measures].[XL_SD0]"]):
            convert2formulas = True
        else:
            convert2formulas = False

        return execute_request_hanlder.generate_response(executor, mdx_query, convert2formulas)


home_directory = expanduser("~")
conf_file = os.path.join(home_directory, "olapy-data", "logs", "xmla.log")


def get_mdx_engine(
    cube_config,
    sql_alchemy_uri,
    olapy_data,
    source_type,
    direct_table_or_file,
    columns,
    measures,
):
    sqla_engine = None
    if sql_alchemy_uri:
        sqla_engine = create_engine(sql_alchemy_uri)

    if direct_table_or_file:
        executor = MdxEngineLite(
            direct_table_or_file=direct_table_or_file,
            source_type=None,
            db_config=None,
            cubes_config=None,
            columns=columns,
            measures=measures,
            sqla_engine=sqla_engine,
        )
        executor.load_cube(table_or_file=direct_table_or_file)
    else:
        executor = MdxEngine(
            olapy_data_location=olapy_data,
            source_type=source_type,
            cube_config=cube_config,
            sqla_engine=sqla_engine,
        )
    return executor


def get_spyne_app(discover_request_hanlder, execute_request_hanlder):
    """

    :param xmla_tools: XmlaDiscoverReqHandler instance
    :return: spyne  Application
    """
    return Application(
        [XmlaProviderService],
        "urn:schemas-microsoft-com:xml-analysis",
        in_protocol=XmlaSoap11(validator="soft"),
        out_protocol=XmlaSoap11(validator="soft"),
        config={"discover_request_hanlder": discover_request_hanlder,
                "execute_request_hanlder": execute_request_hanlder},
    )


def get_wsgi_application(mdx_engine):
    """

    :param mdx_engine: MdxEngine instance
    :return: Wsgi Application
    """
    discover_request_hanlder = XmlaDiscoverReqHandler(mdx_engine)
    execute_request_hanlder = XmlaExecuteReqHandler()
    application = get_spyne_app(discover_request_hanlder, execute_request_hanlder)

    # validator='soft' or nothing, this is important because spyne doesn't
    # support encodingStyle until now !!!!

    return WsgiApplication(application)


@click.command()
@click.option("--host", "-h", default="0.0.0.0", help="Host ip address.")
@click.option("--port", "-p", default=8000, help="Host port.")
@click.option(
    "--write_on_file",
    "-wf",
    default=True,
    help="Write logs into a file or display them into the console. (True : on file)(False : on console)",
)
@click.option(
    "--log_file_path",
    "-lf",
    default=conf_file,
    help="Log file path. DEFAUL : " + conf_file,
)
@click.option(
    "--sql_alchemy_uri",
    "-sa",
    default=None,
    help="SQL Alchemy URI , **DON'T PUT THE DATABASE NAME** ",
)
@click.option(
    "--olapy_data",
    "-od",
    default=os.path.join(expanduser("~"), "olapy-data"),
    help="Olapy Data folder location, Default : ~/olapy-data",
)
@click.option(
    "--source_type",
    "-st",
    default="csv",
    help="Get cubes from where ( db | csv ), DEFAULT : csv",
)
@click.option(
    "--db_config_file",
    "-dbc",
    default=os.path.join(home_directory, "olapy-data", "olapy-config.yml"),
    help="Database configuration file path, DEFAULT : " +
         os.path.join(home_directory, "olapy-data", "olapy-config.yml"),
)
@click.option(
    "--cube_config_file",
    "-cbf",
    default=os.path.join(
        home_directory,
        "olapy-data",
        "cubes",
        "cubes-config.yml",
    ),
    help="Cube config file path, DEFAULT : " +
         os.path.join(home_directory, "olapy-data", "cubes", "cubes-config.yml"),
)
@click.option(
    "--direct_table_or_file",
    "-tf",
    default=None,
    help="File path or db table name if you want to construct cube from a single file (table)",
)
@click.option(
    "--columns",
    "-c",
    default=None,
    help="To explicitly specify columns if (construct cube from a single file), columns order matters ",
)
@click.option(
    "--measures",
    "-m",
    default=None,
    help="To explicitly specify measures if (construct cube from a single file)",
)
def runserver(
    host,
    port,
    write_on_file,
    log_file_path,
    sql_alchemy_uri,
    olapy_data,
    source_type,
    db_config_file,
    cube_config_file,
    direct_table_or_file,
    columns,
    measures,
):
    """
    Start the xmla server.
    """
    try:
        imp.reload(sys)
        # reload(sys)  # Reload is a hack
        sys.setdefaultencoding("UTF8")
    except Exception:
        pass

    cube_config = None
    if cube_config_file and isfile(cube_config_file):
        cube_config_file_parser = ConfigParser()
        cube_config = cube_config_file_parser.get_cube_config(cube_config_file)

    sqla_uri = None
    if "db" in source_type:
        if sql_alchemy_uri:
            # just uri, and inside XmlaDiscoverReqHandler we gonna to change uri if cube changes and the create_engine
            sqla_uri = sql_alchemy_uri
        else:
            # if uri not passed with params, look up in the olapy-data config file
            db_config = DbConfigParser()
            sqla_uri = db_config.get_db_credentials(db_config_file)

    mdx_engine = get_mdx_engine(
        cube_config=cube_config,
        sql_alchemy_uri=sqla_uri,
        olapy_data=olapy_data,
        source_type=source_type,
        direct_table_or_file=direct_table_or_file,
        columns=columns,
        measures=measures,
    )

    wsgi_application = get_wsgi_application(mdx_engine)

    # log to the console
    # logging.basicConfig(level=logging.DEBUG")
    # log to the file
    if write_on_file:
        if not os.path.isdir(
            os.path.join(home_directory, "olapy-data", "logs"), ):
            os.makedirs(os.path.join(home_directory, "olapy-data", "logs"))
        logging.basicConfig(level=logging.DEBUG, filename=log_file_path)
    else:
        logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("spyne.protocol.xml").setLevel(logging.DEBUG)
    logging.info("listening to http://127.0.0.1:8000/xmla")
    logging.info("wsdl is at: http://localhost:8000/xmla?wsdl")
    server = make_server(host, port, wsgi_application)
    server.serve_forever()
