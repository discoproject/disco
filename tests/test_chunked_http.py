from disco.test import TestCase, TestJob
from disco.core import Job
import disco
import threading
import BaseHTTPServer


def map(line, params):
    for word in line.split():
        yield word, 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

PORT = 1234

class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(s):
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.send_header("Transfer-Encoding", "chunked")
        s.end_headers()
        s.wfile.write("b\r\nHello World\r\n0\r\n\r\n")

def startServer():
    server_class = BaseHTTPServer.HTTPServer
    httpd = server_class(('localhost', PORT), MyHandler)
    httpd.handle_request()
    httpd.server_close()

class RawTestCase(TestCase):
    def runTest(self):
        threading.Thread(target=startServer).start()
        input = 'http://localhost:' + str(PORT)
        self.job = Job().run(input=[input], map=map, reduce=reduce)
        self.assertEqual(sorted(self.results(self.job)), [('Hello', 1), ('World', 1)])
