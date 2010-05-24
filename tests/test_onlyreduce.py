from disco.test import DiscoJobTestFixture, DiscoTestCase

import random

class OnlyReduceTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = ['100', '20', '2']

    def getdata(self, path):
        return '%s\n' % path.replace('_', ' ')

    @staticmethod
    def reduce(iter, out, params):
        out.add('', sum(int(v) for k, v in iter))

    @staticmethod
    def reduce_reader(fd, sze, fname):
        for item in re_reader('(.*?)\n', fd, sze, fname):
            yield None, item[0]

    @property
    def answers(self):
        yield '', sum(int(x) for x in self.inputs)
