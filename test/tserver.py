import SocketServer, BaseHTTPServer, SimpleHTTPServer, thread, socket

PORT = 9444

class Server(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):
        allow_reuse_address = True

class Handler(SimpleHTTPServer. SimpleHTTPRequestHandler):
        def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(data_gen(self.path))

def makeurl(inputs):
        host = "http://%s:%d" % (socket.gethostname(), PORT)
        return ["%s/%s" % (host, i) for i in inputs]

def run_server(data_gen0):
        global data_gen
        data_gen = data_gen0
        httpd = Server(('', PORT), Handler)
        thread.start_new_thread(httpd.serve_forever, ())

