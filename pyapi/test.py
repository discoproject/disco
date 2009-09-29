import unittest
from random import randint

from discodb import DiscoDB, Q

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

class TestMappingProtocol(unittest.TestCase):
    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(1000))

    def test_contains(self):
        pass

    def test_length(self):
        assert len(self.discodb) == 1000

    def test_getitem(self):
        for x in xrange(1000):
            try:
                list(self.discodb[str(x)])
            except KeyError:
                assert x == 1000

    def test_iter(self):
        assert list(self.discodb) == list(self.discodb.keys())

    def test_items(self):
        for key, values in self.discodb.items():
            key, list(values)

    def test_keys(self):
        len(list(self.discodb.keys()))

    def test_values(self):
        len(list(self.discodb.values()))

    def test_query(self):
        q = Q.parse('5 & 10 & (15 | 30)')
        list(self.discodb.query(q))

class TestLargeMappingProtocol(TestMappingProtocol):
    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(10000))

class TestSerializationProtocol(unittest.TestCase):
    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(10000))

    def test(self):
        dbuffer = self.discodb.dumps()
        assert dbuffer == DiscoDB.loads(dbuffer).dumps()

class TestLargeSerializationProtocol(TestSerializationProtocol):
    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(10000))


if __name__ == '__main__':
    unittest.main()
