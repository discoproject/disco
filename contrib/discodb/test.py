import doctest, unittest
from random import randint

from discodb import DiscoDB, Q
from discodb import DiscoDBConstructor
from discodb import query
from discodb import tools

def k_vs_iter(N, max_values=100):
    for x in xrange(N):
        # Force every key to have at least ONE value so that the test in
        # test_query_noresults doesn't erroneously pass. The problem there is
        # that queries that should return no results were returning the very
        # first key's value. So, if the first key has no value, then the test
        # could pass but we wouldn't know if it passed because the query truly
        # returned no results (as it should) or if it actually returned the
        # first key's value, which was also empty.
        yield '%s' % x, ('%s' % v for v in xrange(randint(1, max_values)))

class TestConstructor(unittest.TestCase):
    def test_null_constructor(self):
        discodb = DiscoDB()

    def test_dict_constructor(self):
        discodb = DiscoDB(dict(k_vs_iter(1000)))

    def test_list_constructor(self):
        discodb = DiscoDB(list(k_vs_iter(1000)))

    def test_iter_constructor(self):
        discodb = DiscoDB(k_vs_iter(1000))

    def test_kviter_constructor(self):
        discodb = DiscoDB((k, v) for k, vs in k_vs_iter(1000) for v in vs)

    def test_flags_constructor(self):
        discodb = DiscoDB(k_vs_iter(1000), disable_compression=True)
        discodb = DiscoDB(k_vs_iter(1000), unique_items=True)

    def test_external_constructor(self):
        discodb_constructor = DiscoDBConstructor()
        for k, vs in k_vs_iter(1000):
            discodb_constructor.add(k, vs)
        discodb = discodb_constructor.finalize()

class TestMappingProtocol(unittest.TestCase):
    numkeys = 1000

    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(self.numkeys))

    def test_contains(self):
        assert "0" in self.discodb
        assert "key" not in self.discodb

    def test_nonzero(self):
        self.assertFalse(self.discodb.query('NONKEY'))
        self.assertTrue(self.discodb.query('0'))
        self.assertTrue(self.discodb.values())
        self.assertTrue(self.discodb.keys())

    def test_length(self):
        self.assertEquals(len(self.discodb), self.numkeys)

    def test_get(self):
        len(list(self.discodb.get('0')))
        self.assertEquals(self.discodb.get('X'), None)
        self.assertEquals(self.discodb.get('X', 'Y'), 'Y')

    def test_getitem(self):
        for x in xrange(self.numkeys):
            try:
                list(self.discodb[str(x)])
            except KeyError:
                self.assertEquals(x, self.numkeys)

    def test_iter(self):
        self.assertEquals(list(self.discodb), list(self.discodb.keys()))

    def test_items(self):
        for key, values in self.discodb.items():
            key, list(values)

    def test_keys(self):
        len(list(self.discodb.keys()))

    def test_values(self):
        len(list(self.discodb.values()))

    def test_unique_values(self):
        len(list(self.discodb.unique_values()))

    def test_peek(self):
        self.assertNotEquals(self.discodb.peek('0'), None)
        self.assertEquals(self.discodb.peek('X'), None)
        self.assert_(int(self.discodb.peek('0', '1')) >= 0)

    def test_query(self):
        q = Q.parse('5 & 10 & (15 | 30)')
        list(self.discodb.query(q))

    def test_query_results(self):
        q = Q.parse('5')
        self.assertEquals(list(self.discodb.query(q)),
                          list(self.discodb.get('5')))

    def test_query_results_nonkey(self):
        q = Q.parse('nonkey')
        self.assertEquals(list(self.discodb.query(q)), [])

    def test_str(self):
        repr(self.discodb)
        str(self.discodb)

class TestLargeMappingProtocol(TestMappingProtocol):
    numkeys = 10000

class TestSerializationProtocol(unittest.TestCase):
    numkeys = 10000

    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(self.numkeys))

    def test_dumps_loads(self):
        dbuffer = self.discodb.dumps()
        self.assertEquals(dbuffer, DiscoDB.loads(dbuffer).dumps())

    def test_dump_load(self):
        from tempfile import NamedTemporaryFile
        handle = NamedTemporaryFile()
        self.discodb.dump(handle)
        handle.seek(0)
        discodb = DiscoDB.load(handle)
        self.assertEquals(discodb.dumps(), self.discodb.dumps())

class TestLargeSerializationProtocol(TestSerializationProtocol):
    numkeys = 10000

class TestUncompressed(TestMappingProtocol, TestSerializationProtocol):
    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(self.numkeys),
                               disable_compression=True)
        self.discodb_c = DiscoDB(self.discodb)

    def test_compression(self):
        self.assertEqual(dict((k, list(vs)) for k, vs in self.discodb.items()),
                         dict((k, list(vs)) for k, vs in self.discodb_c.items()))

class TestUniqueItems(TestMappingProtocol, TestSerializationProtocol):
    def setUp(self):
        base = dict(k_vs_iter(self.numkeys))
        base['0'] = ['1', '1', '2']
        self.discodb = DiscoDB(base, unique_items=True)

    def test_uniq(self):
        self.assertEqual(list(self.discodb['0']), ['1', '2'])

class TestQuery(unittest.TestCase):
    def setUp(self):
        self.discodb = DiscoDB(
            (('alice', ('blue',)),
            ('bob', ('red',)),
            ('carol', ('blue', 'red'))),
        )

    def q(self, s):
        return self.discodb.query(Q.parse(s))

    def test_empty(self):
        self.assertEqual(list(self.q('')), [])
        self.assertEqual(len(self.q('')), 0)

    def test_get_len(self):
        self.assertEqual(len(self.discodb.get('alice')), 1)
        self.assertEquals(len(self.discodb.get('bob')), 1)
        self.assertEquals(len(self.discodb.get('carol')), 2)

    def test_query_len(self):
        self.assertEquals(len(self.q('alice')), 1)
        self.assertEquals(len(self.q('bob')), 1)
        self.assertEquals(len(self.q('carol')), 2)
        self.assertEquals(len(self.q('alice & bob')), 0)
        self.assertEquals(len(self.q('alice | bob')), 2)
        self.assertEquals(len(self.q('alice & carol')), 1)
        self.assertEquals(len(self.q('alice | carol')), 2)
        self.assertEquals(len(self.q('alice|bob|carol')), 2)
        self.assertEquals(len(self.q('alice&bob&carol')), 0)

    def test_query_len_doesnt_advance_iter(self):
        # check that calling len() doesn't advance the iterator
        res = self.q('alice')
        self.assertEquals(len(res), 1)
        self.assertEquals(len(res), 1)

    def test_query_results(self):
        self.assertEquals(set(self.q('alice')), set(['blue']))
        self.assertEquals(set(self.q('bob')), set(['red']))
        self.assertEquals(set(self.q('carol')), set(['blue', 'red']))
        self.assertEquals(set(self.q('alice & bob')), set())
        self.assertEquals(set(self.q('alice | bob')), set(['blue', 'red']))
        self.assertEquals(set(self.q('alice & carol')), set(['blue']))
        self.assertEquals(set(self.q('alice | carol')), set(['blue', 'red']))
        self.assertEquals(set(self.q('alice|bob|carol')), set(['blue', 'red']))
        self.assertEquals(set(self.q('alice&bob&carol')), set())

    def test_query_len_nonkey(self):
        self.assertEquals(len(self.q('nonkey')), 0)
        self.assertEquals(len(self.q('~nonkey')), 2)
        self.assertEquals(len(self.q('nonkey & alice')), 0)
        self.assertEquals(len(self.q('nonkey | alice')), 1)

    def test_query_results_nonkey(self):
        self.assertEquals(set(self.q('nonkey')), set())
        self.assertEquals(set(self.q('~nonkey')), set(['blue', 'red']))
        self.assertEquals(set(self.q('nonkey & alice')), set())
        self.assertEquals(set(self.q('nonkey | alice')), set(['blue']))


if __name__ == '__main__':
    unittest.TextTestRunner().run(doctest.DocTestSuite(query))
    unittest.TextTestRunner().run(doctest.DocTestSuite(tools))
    unittest.main()
