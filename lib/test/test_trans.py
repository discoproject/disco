import pickle
import unittest
from random import random
from disco.util import encode, decode

class TransTest(unittest.TestCase):
    def transform(self, s):
        pickled = pickle.dumps(s, 2)
        self.assertEqual(pickled, decode(encode(pickled)))

    def test_0(self):
        self.transform(u'\x00\x01\x00\x01\x00')

    def test_1(self):
        self.transform(u'\x00\x00')

    def test_1(self):
        self.transform(u'\xff\xff')

    def test_rand(self):
        for i in range(1000):
            tokens = []
            items = [b'\x00', b'\x01', b'\x02']
            for i in range(200):
                tokens.append(items[int(random() * len(items))])
            s = u''.join(tokens)
            self.transform(s)
