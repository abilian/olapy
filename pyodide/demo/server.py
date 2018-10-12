from wsgiref.simple_server import make_server


def content_type(path):
    if path.endswith(".css"):
        return "text/css"
    elif path.endswith(".js"):
        return "text/javascript"
    elif path.endswith('.wasm'):
        return 'application/wasm'
    elif path.endswith('.csv'):
        return 'text/csv'
    else:
        return "text/html"


def app(environ, start_response):
    path_info = environ["PATH_INFO"]

    if path_info == '/':
        path_info = "/olapy.html"

    resource = "_build" + path_info

    headers = []
    headers.append(("Content-Type", content_type(path_info.split("/")[-1])))
    headers.append(('Access-Control-Allow-Origin', '*'))

    with open(resource, "rb") as f:
        resp_file = f.read()

    start_response("200 OK", headers)
    return [resp_file]


def runserver(environ, start_response):
    # http://127.0.0.1:8080/cubes/sales/Facts.csv
    server = make_server("0.0.0.0", 8080, app)
    server.serve_forever()
