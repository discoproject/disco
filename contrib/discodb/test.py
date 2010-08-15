import doctest, unittest
from random import randint

from discodb import DiscoDB, DiscoDict, MetaDB, Q
from discodb import DiscoDBConstructor
from discodb import query
from discodb import tools

def k_vs_iter(N, max_values=100):
    for x in xrange(N):
        yield '%s' % x, ('%s' % v for v in xrange(randint(0, max_values)))

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

class TestDiscoDict(TestMappingProtocol):
    def setUp(self):
        self.discodb = DiscoDict(k_vs_iter(self.numkeys))

    def test_query(self):
        pass

class TestMetaDBMappingProtocol(TestMappingProtocol):
    def setUp(self):
        datadb = DiscoDB(k_vs_iter(self.numkeys))
        metadb = DiscoDB(k_vs_iter(self.numkeys, max_values=self.numkeys))
        self.discodb = MetaDB(datadb, metadb)

    def test_peek(self):
        pass

class TestMetaDBSerializationProtocol(unittest.TestCase):
    numkeys = 1000

    def setUp(self):
        datadb = DiscoDB(k_vs_iter(self.numkeys))
        metadb = DiscoDB(k_vs_iter(self.numkeys, max_values=self.numkeys))
        self.metadb = MetaDB(datadb, metadb)

    def test_dump_load(self):
        from tempfile import NamedTemporaryFile
        handle = NamedTemporaryFile()
        self.metadb.dump(handle)
        def metavaldict(metavals):
            return dict((k, list(v)) for k, v in metavals)
        handle.seek(0)
        metadb = MetaDB.load(handle)
        assert metavaldict(metadb.values()) == metavaldict(self.metadb.values())
        handle.seek(0)
        metadb = MetaDB.load(handle.name)
        assert metavaldict(metadb.values()) == metavaldict(self.metadb.values())

if __name__ == '__main__':
    unittest.TextTestRunner().run(doctest.DocTestSuite(query))
    unittest.TextTestRunner().run(doctest.DocTestSuite(tools))
    unittest.main()
