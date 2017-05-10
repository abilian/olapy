from spyne import Unicode, Iterable, Application
from spyne.decorator import rpc
from spyne.protocol.http import HttpRpc
from spyne.protocol.json import JsonDocument
from spyne.server.wsgi import WsgiApplication
from spyne.service import ServiceBase

from core.mdx.executor.execute import MdxEngine


class OlapyService(ServiceBase):

    executer = MdxEngine('sales')


    # curl "http://localhost:8000/get_star_schema"
    @rpc(_returns=Unicode)
    def get_star_schema(ctx):
        return OlapyService.executer.load_star_schema_dataframe.to_json()

    # curl "http://localhost:8000/get_cubes_names"
    @rpc(_returns=Unicode)
    def get_all_tables(ctx):
        return {
            table_name: table_dataframe.to_json()
            for table_name, table_dataframe in
            OlapyService.executer.tables_loaded.items()
        }

    # curl "http://localhost:8000/get_measures"
    @rpc(_returns=Iterable(Unicode))
    def get_measures(ctx):
        return OlapyService.executer.measures


application = Application(
    [OlapyService],
    tns='spyne.olapy.services',
    in_protocol=HttpRpc(validator='soft'),
    out_protocol=JsonDocument())

if __name__ == '__main__':
    # You can use any Wsgi server. Here, we chose
    # Python's built-in wsgi server but you're not
    # supposed to use it in production.
    from wsgiref.simple_server import make_server

    wsgi_app = WsgiApplication(application)
    server = make_server('0.0.0.0', 8000, wsgi_app)
    server.serve_forever()
