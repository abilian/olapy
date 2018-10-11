from wsgiref.simple_server import make_server
import os


def content_type(path):
    if path.endswith(".css"):
        return "text/css"
    elif path.endswith(".js"):
        return "text/javascript"
    else:
        return "text/html"


def app(environ, start_response):
    path_info = environ["PATH_INFO"]
    resource = path_info.split("/")[1]

    headers = []
    headers.append(("Content-Type", content_type(resource)))

    if not resource:
        resource = "olapy.html"

    resp_file = os.path.join("_build", resource)

    try:
        with open(resp_file, "rb") as f:
            resp_file = f.read()
    except Exception:
        start_response("404 Not Found", headers)
        return ["404 Not Found"]

    start_response("200 OK", headers)
    return [resp_file]


def runserver(environ, start_response):
    server = make_server("0.0.0.0", 8080, app)
    server.serve_forever()
