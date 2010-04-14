from disco.test import DiscoJobTestFixture, DiscoTestCase

from disco.ddfs import DDFS
from disco.util import ddfs_name

class DDFSSaveTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31]
    save   = True

    def getdata(self, path):
        return '\n'.join([path] * 10)

    @staticmethod
    def map(e, params):
        return [('=' + e, e)]

    @staticmethod
    def reduce(iter, out, params):
        s = 1
        for k, v in iter:
            assert k == '=' + v, "Corrupted key!"
            s *= int(v)
        out.add('result', s)

    @property
    def answers(self):
        yield ('result',
               '1028380578493512611198383005758052057919386757620401'
               '58350002406688858214958513887550465113168573010369619140625')

    def tearDown(self):
        super(DDFSSaveTestCase, self).tearDown()
        DDFS(self.disco_master_url).delete(ddfs_name(self.job.name))

class DDFSWriteTestCase(DiscoTestCase):
    def setUp(self):
        self.ddfs = DDFS(self.disco_master_url)

    def test_push(self):
        from cStringIO import StringIO
        self.ddfs.push('disco:test:blobs', [(StringIO('blobdata'), 'blobdata')])
        self.ddfs.delete('disco:test:blobs')

    def test_tag(self):
        self.ddfs.tag('disco:test:tag', [['urls']])
        self.ddfs.delete('disco:test:tag')

    def test_delete(self):
        self.ddfs.delete('disco:test:notag')

class DDFSReadTestCase(DiscoTestCase):
    def setUp(self):
        from cStringIO import StringIO
        self.ddfs = DDFS(self.disco_master_url)
        self.ddfs.push('disco:test:blobs', [(StringIO('blobdata'), 'blobdata')])
        self.ddfs.tag('disco:test:tag', [['urls']])
        self.ddfs.tag('disco:test:metatag',
                      [['tag://disco:test:tag'], ['tag://disco:test:metatag']])

    def test_blobs(self):
        from os.path import basename
        blobs = list(self.ddfs.blobs('disco:test:blobs'))
        self.assert_(basename(blobs[0][0]).startswith('blobdata'))
        self.assertCommErrorCode(404,
                                 lambda: self.ddfs.blobs('disco:test:notag',
                                                         ignore_missing=False))
        self.assertEquals(list(self.ddfs.blobs('disco:test:notag')), [])

    def test_exists(self):
        self.assertEquals(self.ddfs.exists(''), False)
        self.assertEquals(self.ddfs.exists('!!'), False)
        self.assertEquals(self.ddfs.exists('disco:test:tag'), True)
        self.assertEquals(self.ddfs.exists('disco:test:notag'), False)

    def test_findtags(self):
        list(self.ddfs.findtags(['disco:test:metatag']))

    def test_get(self):
        self.assertCommErrorCode(403, lambda: self.ddfs.get(''))
        self.assertCommErrorCode(404, lambda: self.ddfs.get('disco:test:notag'))
        self.assertEquals(self.ddfs.get('disco:test:tag')['urls'], [['urls']])
        self.assertEquals(self.ddfs.get(['disco:test:tag'])['urls'], [['urls']])

    def test_list(self):
        self.assert_('disco:test:tag' in self.ddfs.list())
        self.assert_('disco:test:tag' in self.ddfs.list('disco:test'))
        self.assertEquals(self.ddfs.list('disco:test:notag'), [])

    def test_pull(self):
        self.ddfs.pull('disco:test:tag')

    def test_walk(self):
        list(self.ddfs.walk('disco:test:tag'))

    def tearDown(self):
        self.ddfs.delete('disco:test:blobs')
        self.ddfs.delete('disco:test:tag')
        self.ddfs.delete('disco:test:metatag')
