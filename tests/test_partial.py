from functools import partial

from disco.test import TestCase, TestJob
from disco.worker.classic.worker import Params

def foo(x, extra):
    return x + extra

def init(items, params, extra):
    pass

def reader(fd, size, fname, extra):
    from disco.worker.classic.func import chain_reader
    for k, v in chain_reader(fd, size, fname):
        yield k + extra, v

def map(e, params, extra):
    yield e[0] + params.foo(extra), e[1]

def combiner(k, v, buf, done, params, extra):
    if not done:
        return [(k + extra, v)]

def reduce(items, out, params, extra):
    for k, v in items:
        out.add(k + params.foo(extra), v)

class PartialJob(TestJob):
    map_init = partial(init, extra='d')
    map = partial(map, extra='a')
    combiner = partial(combiner, extra='b')
    reduce_init = partial(init, extra='e')
    reduce = partial(reduce, extra='c')
    map_reader = partial(reader, extra='f')
    reduce_reader = partial(reader, extra='h')
    params = Params(foo=partial(foo, extra='z'))

class PartialTestCase(TestCase):
    def serve(self, path):
        return '1 _ 0 \n'

    def runTest(self):
        self.job = PartialJob().run(input=self.test_server.urls(range(self.num_workers)))
        for k, v in self.results(self.job):
            self.assertEquals(k, '_fazbhcz')
