from disco.test import DiscoJobTestFixture, DiscoTestCase

from cStringIO import StringIO

class AuthJobTestCase(DiscoJobTestFixture, DiscoTestCase):
    input = []

    @staticmethod
    def map(e, params):
        return [(e.strip(), '')]

    @property
    def answers(self):
        return [('blobdata', '')]

    def setUp(self):
        tag = 'disco:test:authjob'
        pushed = self.ddfs.push(tag, [(StringIO('blobdata'), 'blob')])
        self.ddfs.setattr(tag, 'ddfs:read-token', 'r')
        self.input = ['tag://u:r@/' + tag]
        super(AuthJobTestCase, self).setUp()

    def tearDown(self):
        super(AuthJobTestCase, self).tearDown()
        self.ddfs.delete('disco:test:authjob')
