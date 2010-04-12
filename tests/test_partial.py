from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.core import Params
from functools import partial

def foo(x, extra):
    return x+extra

def init(items, params, extra):
    pass

def reader(fd, size, fname, extra):
    from disco.func import netstr_reader
    for k,v in netstr_reader(fd, size, fname):
        yield k+extra, v

def writer(fd, k, v, params, extra):
    from disco.func import netstr_writer
    netstr_writer(fd, k+extra, v, params)

def map(e, params, extra):
    return [(e[0]+params.foo(extra), e[1])]

def combiner(k, v, buf, done, params, extra):
    if not done:
        return [(k+extra, v)]

def reduce(items, out, params, extra):
    for k,v in items:
        out.add(k+params.foo(extra), v)

class PartialTestCase(DiscoJobTestFixture, DiscoTestCase):
    @property
    def inputs(self):
        return [str(x) for x in range(self.num_workers)]

    def getdata(self, path):
        return '1 _ 0 \n'

    map=partial(map, extra='a')
    combiner=partial(combiner, extra='b')
    reduce=partial(reduce, extra='c')
    map_init=partial(init, extra='d')
    reduce_init=partial(init, extra='e')
    map_reader=partial(reader, extra='f')
    map_writer=partial(writer, extra='g')
    reduce_reader=partial(reader, extra='h')
    reduce_writer=partial(writer, extra='i')
    params=Params(foo=partial(foo, extra='z'))

    def runTest(self):
        for k, v in self.results:
            self.assertEquals(k, '_fazbghczi')
