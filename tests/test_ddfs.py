from disco.test import TestCase

from cStringIO import StringIO

class DDFSUpdateTestCase(TestCase):
    data = StringIO('blobdata')

    def blobnames(self, tag):
        from disco.ddfs import DDFS
        return list(reversed(list(DDFS.blob_name(repl[0])
                                  for repl in self.ddfs.blobs(tag))))

    def test_update(self):
        for i in range(5):
            self.ddfs.push('disco:test:blobs',
                           [(self.data, 'dup')] * 2,
                           update=True)
        self.assertEquals(len(self.blobnames('disco:test:blobs')), 1)
        for i in range(5):
            self.ddfs.push('disco:test:blobs',
                           [(self.data, 'dup2')],
                           update=True,
                           delayed=True)
        self.assertEquals(len(self.blobnames('disco:test:blobs')), 2)
        self.ddfs.delete('disco:test:blobs')

    def test_random(self):
        import random
        keys = [str(random.randint(1, 100)) for i in range(100)]
        ukeys = []
        for key in keys:
            self.ddfs.push('disco:test:blobs', [(self.data, key)], update=True)
            if key not in ukeys:
                ukeys.append(key)
        self.assertEquals(ukeys, self.blobnames('disco:test:blobs'))
        self.ddfs.delete('disco:test:blobs')

    def test_mixed(self):
        keys = []
        for key in map(str, range(10)):
            self.ddfs.push('disco:test:blobs', [(self.data, key)] * 2)
            keys += [key] * 2
        for key in map(str, range(15)):
            self.ddfs.push('disco:test:blobs',
                           [(self.data, key)] * 2,
                           update=True)
            if int(key) > 9:
                keys.append(key)
        for key in map(str, range(10)):
            self.ddfs.push('disco:test:blobs',
                           [(self.data, key)] * 2,
                           delayed=True)
            keys += [key] * 2
        self.assertEquals(keys, self.blobnames('disco:test:blobs'))
        self.ddfs.delete('disco:test:blobs')

    def tearDown(self):
        self.ddfs.delete('disco:test:blobs')

class DDFSWriteTestCase(TestCase):
    def test_chunk(self):
        from disco.core import classic_iterator
        url = 'http://discoproject.org/media/text/chekhov.txt'
        self.ddfs.chunk('disco:test:chunk', [url], chunk_size=100*1024)
        self.assert_(0 < len(list(self.ddfs.blobs('disco:test:chunk'))) <= 4)
        self.assert_(list(classic_iterator(['tag://disco:test:chunk'])),
                     list(classic_iterator([url], reader=None)))
        self.ddfs.delete('disco:test:chunk')

    def test_push(self):
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

    def tearDown(self):
        self.ddfs.delete('disco:test:notag')
        self.ddfs.delete('disco:test:tag')
        self.ddfs.delete('disco:test:blobs')
        self.ddfs.delete('disco:test:blobs2')

class DDFSReadTestCase(TestCase):
    def setUp(self):
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
        self.assertEquals([(self.ddfs.blob_name(fd.url), fd.read())
                           for fd  in self.ddfs.pull('disco:test:blobs')],
                          [('blobdata2', 'datablob2'), ('blobdata', 'datablob')])
        self.assertEquals([(self.ddfs.blob_name(fd.url), fd.read())
                           for fd in self.ddfs.pull('disco:test:blobs',
                                                    blobfilter=lambda b: '2' in b)],
                          [('blobdata2', 'datablob2')])
        self.assertEquals([(len(fd), fd.read()) for fd in
                           self.ddfs.pull('disco:test:emptyblob')], [(0, '')])
        self.assertCommErrorCode(404, self.ddfs.pull('disco:test:notag').next)

    def test_exists(self):
        self.assertEquals(self.ddfs.exists('disco:test:tag'), True)
        self.assertEquals(self.ddfs.exists('disco:test:notag'), False)
        self.assertEquals(self.ddfs.exists('tag://disco:test:tag'), True)
        self.assertEquals(self.ddfs.exists('tag://disco:test:notag'), False)

    def test_findtags(self):
        list(self.ddfs.findtags(['disco:test:metatag']))

    def test_get(self):
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


class DDFSAttrTestCase(TestCase):
    def setUp(self):
        self.ddfs.push('disco:test:attrs', [(StringIO('datablob'), 'blobdata')])
        self.ddfs.setattr('disco:test:attrs', 'a1', 'v1')
        self.ddfs.setattr('disco:test:attrs', 'a2', 'v2')

    def test_setattr(self):
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a1'), 'v1')
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a2'), 'v2')

    def test_attrs(self):
        self.assertEquals(self.ddfs.attrs('disco:test:attrs'), {'a1':'v1',
                                                                'a2':'v2'})

    def test_delattr(self):
        self.ddfs.setattr('disco:test:attrs', 'a3', 'v3')
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a1'), 'v1')
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a2'), 'v2')
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a3'), 'v3')
        self.assertEquals(self.ddfs.attrs('disco:test:attrs'), {'a1':'v1',
                                                                'a2':'v2',
                                                                'a3':'v3'})
        self.ddfs.delattr('disco:test:attrs', 'a3')
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a1'), 'v1')
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a2'), 'v2')
        self.assertEquals(self.ddfs.attrs('disco:test:attrs'), {'a1':'v1',
                                                                'a2':'v2'})

    def test_delattr_unknown(self):
        self.ddfs.delattr('disco:test:attrs', 'z')

    def test_resetattr(self):
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a1'), 'v1')
        self.ddfs.setattr('disco:test:attrs', 'a1', 'v1.2')
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a1'), 'v1.2')
        self.ddfs.setattr('disco:test:attrs', 'a1', 'v1')
        self.assertEquals(self.ddfs.getattr('disco:test:attrs', 'a1'), 'v1')

    def test_reserved_attrs(self):
        setter = lambda: self.ddfs.setattr('disco:test:attrs', 'ddfs:a1', 'v')
        self.assertCommErrorCode(404, setter)

    def tearDown(self):
        self.ddfs.delete('disco:test:attrs')

class DDFSAuthTestCase(TestCase):
    def setUp(self):
        self.ddfs.push('disco:test:authrd', [(StringIO('datablob'), 'blobdata')])
        self.ddfs.push('disco:test:authwr', [(StringIO('datablob'), 'blobdata')])
        self.ddfs.push('disco:test:authempty', [(StringIO('datablob'), 'blobdata')])
        self.ddfs.setattr('disco:test:authrd', 'a', 'v')
        self.ddfs.setattr('disco:test:authwr', 'a', 'v')
        self.ddfs.setattr('disco:test:authrd', 'ddfs:read-token', 'rdr')
        self.ddfs.setattr('disco:test:authwr', 'ddfs:write-token', 'wtr')
        self.ddfs.setattr('disco:test:authempty', 'a', 'v')
        self.ddfs.setattr('disco:test:authempty', 'ddfs:read-token', '')
        self.ddfs.setattr('disco:test:authempty', 'ddfs:write-token', '')

    def test_empty(self):
        self.assertEquals(self.ddfs.getattr('disco:test:authempty', 'a', token=''), 'v')
        self.ddfs.setattr('disco:test:authempty', 'a2', 'v2', token='')
        self.assertEquals(self.ddfs.getattr('disco:test:authempty', 'a2', token=''), 'v2')
        self.ddfs.delattr('disco:test:authempty', 'a2', token='')

    def test_write_noread(self):
        self.assertEquals(self.ddfs.getattr('disco:test:authwr', 'a'), 'v')
        self.assertEquals(self.ddfs.getattr('disco:test:authwr', 'a', token='rand'), 'v')

    def test_write_noread2(self):
        self.assertCommErrorCode(401, lambda: self.ddfs.setattr('disco:test:authwr', 'a2', 'v2'))
        rand_setter = lambda: self.ddfs.setattr('disco:test:authwr', 'a2', 'v2', token='rand')
        self.assertCommErrorCode(401, rand_setter)
        self.ddfs.setattr('disco:test:authwr', 'a2', 'v2', token='wtr')
        self.ddfs.delattr('disco:test:authwr', 'a2', token='wtr')

    def test_write_noread3(self):
        setter = lambda: self.ddfs.setattr('disco:test:authwr', 'ddfs:read-token', 'r')
        self.assertCommErrorCode(401, setter)
        self.ddfs.setattr('disco:test:authwr', 'ddfs:read-token', 'r', token='wtr')
        self.assertCommErrorCode(401, lambda: self.ddfs.getattr('disco:test:authwr', 'a'))
        self.assertEquals(self.ddfs.getattr('disco:test:authwr', 'a', token='r'), 'v')
        self.ddfs.delattr('disco:test:authwr', 'ddfs:read-token', token='wtr')

    def test_read_nowrite(self):
        self.assertCommErrorCode(401, lambda: self.ddfs.getattr('disco:test:authrd', 'a'))
        rand_getter = lambda: self.ddfs.getattr('disco:test:authrd', 'a', token='rand')
        self.assertCommErrorCode(401, rand_getter)
        self.assertEquals(self.ddfs.getattr('disco:test:authrd', 'a', token='rdr'), 'v')

    def test_read_nowrite2(self):
        self.ddfs.setattr('disco:test:authrd', 'a2', 'v2')
        self.assertEquals(self.ddfs.getattr('disco:test:authrd', 'a2', token='rdr'), 'v2')
        self.ddfs.delattr('disco:test:authrd', 'a2', token='rand')

    def test_read_nowrite3(self):
        self.ddfs.setattr('disco:test:authrd', 'ddfs:read-token', 'r')
        self.ddfs.setattr('disco:test:authrd', 'ddfs:read-token', 'rdr')

    def test_atomic_token(self):
        self.ddfs.push('disco:test:atomic1',
                        [(StringIO('abc'), 'atom')],
                        update=True,
                        delayed=True,
                        token='secret1')
        getter = lambda: self.ddfs.getattr('disco:test:atomic1', 'foobar')
        self.assertCommErrorCode(401, getter)
        self.assertEquals(self.ddfs.getattr('disco:test:atomic1',
                                            'ddfs:write-token',
                                            token='secret1'), 'secret1')
        self.ddfs.put('disco:test:atomic2', [], token='secret2')
        getter = lambda: self.ddfs.getattr('disco:test:atomic2', 'foobar')
        self.assertCommErrorCode(401, getter)
        self.assertEquals(self.ddfs.getattr('disco:test:atomic2',
                                            'ddfs:write-token',
                                            token='secret2'), 'secret2')
        self.ddfs.put('disco:test:notoken', [])
        self.assertEquals(self.ddfs.getattr('disco:test:notoken',
                                            'ddfs:write-token'), None)

    def tearDown(self):
        self.ddfs.delete('disco:test:authrd')
        self.ddfs.delete('disco:test:authwr', token='wtr')
        self.ddfs.delete('disco:test:authempty', token='')
        self.ddfs.delete('disco:test:atomic1', token='secret1')
        self.ddfs.delete('disco:test:atomic2', token='secret2')
        self.ddfs.delete('disco:test:notoken')

class DDFSDeleteTestCase(TestCase):
    def setUp(self):
        self.ddfs.push('disco:test:delete1', [(StringIO('datablob'), 'blobdata')])
        self.ddfs.push('disco:test:delete2', [(StringIO('datablob'), 'blobdata')])

    def test_create_delete_create(self):
        self.ddfs.delete('disco:test:delete1')
        self.assert_(not self.ddfs.exists('disco:test:delete1'))
        self.ddfs.push('disco:test:delete1', [(StringIO('datablob'), 'blobdata')])
        self.assert_(self.ddfs.exists('disco:test:delete1'))
        self.assert_("disco:test:delete1" in self.ddfs.list('disco:test:delete1'))

    def test_create_delete_create_token(self):
        self.ddfs.delete('disco:test:delete2')
        self.assert_(not self.ddfs.exists('disco:test:delete2'))
        self.ddfs.push('disco:test:delete2',
                       [(StringIO('abc'), 'atom')],
                       token='secret1')
        self.assert_(self.ddfs.exists('disco:test:delete2'))
        self.assert_("disco:test:delete2" in self.ddfs.list('disco:test:delete2'))

    def tearDown(self):
        self.ddfs.delete('disco:test:delete1')
        self.ddfs.delete('disco:test:delete2', token='secret1')
