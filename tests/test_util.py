import os
from datetime import datetime

from disco.test import TestCase
from disco.util import flatten, iterify, urlsplit

def function(x):
    return x + 0

sequence = 0, [1, [2, 3], [[4, [5, [6]]]]]

class UtilTestCase(TestCase):
    def test_flatten(self):
        self.assertEquals(range(7), list(flatten(sequence)))

    def test_iterify(self):
        self.assertEquals([5], list(iterify(5)))
        self.assertEquals([5], list(iterify([5])))

    def test_urlsplit(self):
        port = self.settings['DISCO_PORT']
        ddfs = self.settings['DDFS_DATA']
        data = self.settings['DISCO_DATA']
        self.assertEquals(urlsplit('http://host/path'),
                          ('http', ('host', ''), 'path'))
        self.assertEquals(urlsplit('http://host:port/path'),
                          ('http', ('host', 'port'), 'path'))
        self.assertEquals(urlsplit('disco://master/long/path'),
                          ('http', ('master', '%s' % port), 'long/path'))
        self.assertEquals(urlsplit('disco://localhost/ddfs/path',
                                   localhost='localhost',
                                   ddfs_data=ddfs),
                          ('file', ('', ''), os.path.join(ddfs, 'path')))
        self.assertEquals(urlsplit('disco://localhost/data/path',
                                   localhost='localhost',
                                   disco_data=data),
                          ('file', ('', ''), os.path.join(data, 'path')))
        self.assertEquals(urlsplit('tag://tag', ''),
                          ('tag', ('', ''), 'tag'))
        self.assertEquals(urlsplit('tag://host/tag', ''),
                          ('tag', ('host', ''), 'tag'))
        self.assertEquals(urlsplit('tag://host:port/tag', ''),
                          ('tag', ('host', 'port'), 'tag'))
