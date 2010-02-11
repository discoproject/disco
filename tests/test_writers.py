from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.func import object_reader, object_writer

class WriterTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31]

    def getdata(self, path):
        return '\n'.join([path] * 10)

    @staticmethod
    def map(e, params):
        return [('=' + e, e)]

    @staticmethod
    def map_writer(fd, key, value, params):
        fd.write('%s|%s\n' % (key, value))

    @staticmethod
    def reduce(iter, out, params):
        s = 1
        for k, v in iter:
            assert k == '=' + v, "Corrupted key!"
            s *= int(v)
        out.add("result", s)

    @staticmethod
    def reduce_reader(fd, size, fname):
        for x in re_reader('(.*?)\n', fd, size, fname, output_tail=True):
            yield x[0].split('|')

    @staticmethod
    def reduce_writer(fd, key, value, params):
        fd.write("<%s>" % value)

    @staticmethod
    def result_reader(fd, size, fname):
        yield fd.read(size)[1:-1]

    @property
    def answers(self):
        yield '1028380578493512611198383005758052057919386757620401'\
              '58350002406688858214958513887550465113168573010369619140625'

class ObjectWriterTestCase(WriterTestCase):
    map_writer    = object_writer
    reduce_reader = object_reader
