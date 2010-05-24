from disco.test import DiscoTestCase
from disco.util import flatten, iterify, rapply, pack, unpack, urlsplit

from datetime import datetime

def function(x):
    return x + 0

sequence = 0, [1, [2, 3], [[4, [5, [6]]]]]

class UtilTestCase(DiscoTestCase):
    def test_flatten(self):
        self.assertEquals(range(7), list(flatten(sequence)))

    def test_iterify(self):
        self.assertEquals([5], list(iterify(5)))
        self.assertEquals([5], list(iterify([5])))

    def test_rapply(self):
        for x, y in zip(xrange(7), flatten(rapply(sequence, function))):
            self.assertEquals(function(x), y)


    def test_pack(self):
        now = datetime.now()
        self.assertEquals(now, unpack(pack(now)))
        self.assertEquals(666, unpack(pack(666)))
        self.assertEquals(function.func_code, unpack(pack(function)).func_code)

    def test_urlsplit(self):
        port = self.disco_settings['DISCO_PORT']
        self.assertEquals(urlsplit('http://host/path'),
                          ('http', ('host', ''), 'path'))
        self.assertEquals(urlsplit('http://host:port/path'),
                          ('http', ('host', 'port'), 'path'))
        self.assertEquals(urlsplit('disco://master/long/path'),
                          ('http', ('master', '%s' % port), 'long/path'))

