from disco.test import TestCase, TestJob
from disco.core import Job
from disco.compat import http_server
import disco
import threading


def map(line, params):
    for word in line.split():
        yield word, 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

PORT = 1234

class MyHandler(http_server.BaseHTTPRequestHandler):
    def do_GET(s):
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.send_header("Transfer-Encoding", "chunked")
        s.end_headers()
        s.wfile.write("b\r\nHello World\r\n0\r\n\r\n")

def startServer():
    server_class = http_server.HTTPServer
    httpd = server_class(('', PORT), MyHandler)
    httpd.handle_request()
    httpd.server_close()

class RawTestCase(TestCase):
    def runTest(self):
        threading.Thread(target=startServer).start()
        input = 'http:' + self.disco.master.split(':')[1] + ":" + str(PORT)
        self.job = Job().run(input=[input], map=map, reduce=reduce)
        self.assertEqual(sorted(self.results(self.job)), [('Hello', 1), ('World', 1)])
