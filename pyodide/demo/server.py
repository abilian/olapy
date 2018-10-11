# Python http.server that sets Access-Control-Allow-Origin header.
# https://gist.github.com/razor-x/9542707

import os
import http.server
import socketserver

PORT = 8080


class HTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        # http.server.SimpleHTTPRequestHandler
        Handler = http.server.SimpleHTTPRequestHandler
        # IMPORTANT
        Handler.extensions_map['.wasm'] = 'application/wasm'
        Handler.end_headers(self)


def server(port):
    httpd = socketserver.TCPServer(('', port), HTTPRequestHandler)
    return httpd


def app(environ, start_response):
    current_folder_path, current_folder_name = os.path.split(os.getcwd())
    if not current_folder_name == '_build':
        os.chdir('_build')
    port = PORT
    httpd = server(port)
    print("\nserving from _build/ at localhost:" + str(port))
    httpd.serve_forever()
