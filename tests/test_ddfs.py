from disco.test import DiscoJobTestFixture, DiscoTestCase

from disco.ddfs import DDFS
from disco.util import ddfs_name

class DDFSWriteTestCase(DiscoTestCase):
    def setUp(self):
        self.ddfs = DDFS(self.disco_master_url)

    def test_push(self):
        from cStringIO import StringIO
        self.ddfs.push('disco:test:blobs', [(StringIO('blobdata'), 'blobdata')])
        self.assert_(self.ddfs.exists('disco:test:blobs'))
        self.ddfs.push('tag://disco:test:blobs2', [(StringIO('blobdata'), 'blobdata')])
        self.assert_(self.ddfs.exists('disco:test:blobs2'))
        self.ddfs.delete('disco:test:blobs')
        self.assert_(not self.ddfs.exists('disco:test:blobs'))
        self.ddfs.delete('disco:test:blobs2')
        self.assert_(not self.ddfs.exists('disco:test:blobs2'))

    def test_tag(self):
        self.ddfs.tag('disco:test:tag', [['urls']])
        self.assert_(self.ddfs.exists('disco:test:tag'))
        self.ddfs.delete('disco:test:tag')
        self.assert_(not self.ddfs.exists('disco:test:tag'))
        self.ddfs.tag('tag://disco:test:tag', [['urls']])
        self.assert_(self.ddfs.exists('tag://disco:test:tag'))
        self.ddfs.tag('disco:test:tag', [['more_urls']])
        self.assertEquals(sorted(self.ddfs.get('disco:test:tag')['urls']),
                          sorted([['urls'], ['more_urls']]))
        self.ddfs.delete('tag://disco:test:tag')
        self.assert_(not self.ddfs.exists('tag://disco:test:tag'))

    def test_put(self):
        self.ddfs.put('disco:test:tag', [['urls']])
        self.assert_(self.ddfs.exists('disco:test:tag'))
        self.assertEquals(self.ddfs.get('disco:test:tag')['urls'], [['urls']])
        self.ddfs.put('disco:test:tag', [['tags']])
        self.assertEquals(self.ddfs.get('disco:test:tag')['urls'], [['tags']])
        self.ddfs.delete('tag://disco:test:tag')

    def test_delete(self):
        self.ddfs.delete('disco:test:notag')

class DDFSReadTestCase(DiscoTestCase):
    def setUp(self):
        from cStringIO import StringIO
        self.ddfs = DDFS(self.disco_master_url)
        self.ddfs.push('disco:test:blobs', [(StringIO('datablob'), 'blobdata')])
        self.ddfs.push('disco:test:blobs', [(StringIO('datablob2'), 'blobdata2')])
        self.ddfs.push('disco:test:emptyblob', [(StringIO(''), 'empty')])
        self.ddfs.tag('disco:test:tag', [['urls']])
        self.ddfs.tag('disco:test:metatag',
                      [['tag://disco:test:tag'], ['tag://disco:test:metatag']])

    def test_blobs(self):
        from os.path import basename
        blobs = list(self.ddfs.blobs('disco:test:blobs'))
        self.assert_(basename(blobs[0][0]).startswith('blobdata'))
        self.assertCommErrorCode(404,
                                 lambda: list(self.ddfs.blobs('disco:test:notag',
                                                         ignore_missing=False)))
        self.assertEquals(list(self.ddfs.blobs('disco:test:notag')), [])

    def test_pull(self):
        self.assertEquals([(self.ddfs.blob_name(url), fd.read())
                           for fd, sze, url in self.ddfs.pull('disco:test:blobs')],
                          [('blobdata2', 'datablob2'), ('blobdata', 'datablob')])
        self.assertEquals([(self.ddfs.blob_name(url), fd.read())
                           for fd, sze, url in self.ddfs.pull('disco:test:blobs',
                                                              blobfilter=lambda b: '2' in b)],
                          [('blobdata2', 'datablob2')])
        self.assertEquals([(sze, fd.read()) for fd, sze, url in
                           self.ddfs.pull('disco:test:emptyblob')], [(0, '')])
        self.assertCommErrorCode(404, self.ddfs.pull('disco:test:notag').next)

    def test_exists(self):
        self.assertEquals(self.ddfs.exists(''), False)
        self.assertEquals(self.ddfs.exists('!!'), False)
        self.assertEquals(self.ddfs.exists('disco:test:tag'), True)
        self.assertEquals(self.ddfs.exists('disco:test:notag'), False)
        self.assertEquals(self.ddfs.exists('tag://disco:test:tag'), True)
        self.assertEquals(self.ddfs.exists('tag://disco:test:notag'), False)

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

    def test_walk(self):
        list(self.ddfs.walk('disco:test:tag'))

    def tearDown(self):
        self.ddfs.delete('disco:test:blobs')
        self.ddfs.delete('disco:test:emptyblob')
        self.ddfs.delete('disco:test:tag')
        self.ddfs.delete('disco:test:metatag')
