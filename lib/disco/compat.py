# Python2/Python3 compatibility.
import sys

if sys.version_info[0] == 3:

    basestring = str

    def bytes_of_int(i):
        return bytes([i])
    def int_of_byte(i):
        return i
    def str_to_bytes(s):
        if isinstance(s, bytes):
            return s
        return s.encode('utf-8')
    def bytes_to_str(b):
        if isinstance(b, str):
            return b
        return b.decode('utf-8')
    def force_utf8(string):
        return string

    from io import StringIO, BytesIO
    from io import FileIO as file

    import pickle
    pickle_loads, pickle_dumps = pickle.loads, pickle.dumps
    pickle_load,  pickle_dump  = pickle.load,  pickle.dump

    import socketserver as socket_server
    from urllib.parse import urlencode
    import http.client as httplib
    import http.server as http_server

else:

    basestring = basestring

    def bytes_of_int(i):
        return chr(int(i))
    def int_of_byte(b):
        return ord(b)
    def str_to_bytes(s):
        return s
    def bytes_to_str(b):
        return b
    def force_utf8(string):
        if isinstance(string, unicode):
            return string.encode('utf-8', 'replace')
        return string.decode('utf-8', 'replace').encode('utf-8')

    from cStringIO import StringIO
    BytesIO = StringIO
    file = file

    import cPickle
    pickle_loads, pickle_dumps = cPickle.loads, cPickle.dumps
    pickle_load,  pickle_dump  = cPickle.load,  cPickle.dump

    import SocketServer as socket_server
    from urllib import urlencode
    import httplib as httplib
    import BaseHTTPServer as http_server
