import SocketServer, BaseHTTPServer, SimpleHTTPServer, thread, socket, os

if "TSERVER_PORT" in os.environ:
        PORT = int(os.environ['TSERVER_PORT'])
else:
        PORT = 9444

class Server(SocketServer.ThreadingMixIn, BaseHTTPServer.HTTPServer):
        allow_reuse_address = True

class Handler(SimpleHTTPServer. SimpleHTTPRequestHandler):
        def do_GET(self):
                d = data_gen(self.path)
                self.send_response(200)
                self.send_header("Content-length", len(d))
                self.end_headers()
                self.wfile.write(d)

def makeurl(inputs):
        host = "http://%s:%d" % (socket.gethostname(), PORT)
        return ["%s/%s" % (host, i) for i in inputs]

def run_server(data_gen0):
        global data_gen
        data_gen = data_gen0
        httpd = Server(('', PORT), Handler)
        thread.start_new_thread(httpd.serve_forever, ())

