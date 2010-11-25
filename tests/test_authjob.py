from disco.test import DiscoJobTestFixture, DiscoTestCase

from disco.ddfs import DDFS
from disco.util import ddfs_name

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
        self.ddfs = DDFS(self.disco_master_url)
        pushed = self.ddfs.push(tag, [(StringIO('blobdata'), 'blob')])
        self.ddfs.setattr(tag, 'ddfs:read-token', 'r')
        self.input = ['tag://u:r@/' + tag]
        super(AuthJobTestCase, self).setUp()

    def tearDown(self):
        super(AuthJobTestCase, self).tearDown()
        self.ddfs.delete('disco:test:authjob')
