import datetime, functools, re

from disco.test import TestCase
from disco.dPickle import dumps, loads

def f():
    def g():
        pass
    return g

def h(*args, **kwargs):
    return args, kwargs

p = functools.partial(h, 1, extra='d')

class PickleTestCase(TestCase):
    def test_dpickle(self):
        now = datetime.datetime.now()
        self.assertEquals(now, loads(dumps(now)))
        self.assertEquals(666, loads(dumps(666)))
        self.assertEquals(f.__code__, loads(dumps(f)).__code__)
        self.assertEquals(f().__code__, loads(dumps(f())).__code__)
        self.assertEquals(p('a', b='b'), loads(dumps(p))('a', b='b'))

    def test_pattern(self):
        pattern = re.compile(b'pattern.*!')
        self.assertEquals(pattern, loads(dumps(pattern)))
