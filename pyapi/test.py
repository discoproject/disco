import unittest
from random import randint

from discodb import DiscoDB

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

    def test_copy(self):
        pass

    def test_contains(self):
        pass

    def test_length(self):
        pass

    def test_getitem(self):
        [self.discodb[str(x)] for x in xrange(100)]

    def test_iter(self):
        pass

    def test_items(self):
        pass

    def test_keys(self):
        pass

    def test_values(self):
        pass

    def test_query(self):
        pass

class TestLargeMappingProtocol(TestMappingProtocol):
    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(10000))

class TestSerializationProtocol(unittest.TestCase):
    def test_dumps(self):
        pass

    def test_dump(self):
        pass

    def test_loads(self):
        DiscoDB.loads("abcdef")

    def test_load(self):
        pass

class TestLargeSerializationProtocol(TestSerializationProtocol):
    def setUp(self):
        self.discodb = DiscoDB(k_vs_iter(10000))


if __name__ == '__main__':
    unittest.main()
