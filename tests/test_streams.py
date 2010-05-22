from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.core import result_iterator
from disco.func import map_input_stream, netstr_writer

def map_input_stream1(stream, size, url, params):
    r = stream.read()
    fd = cStringIO.StringIO('a' + r)
    return fd, len(r) + 1, 'a:%d' % Task.num_partitions

def map_input_stream2(stream, size, url, params):
    r = stream.read()
    fd = cStringIO.StringIO('b' + r)
    return fd, len(r) + 1, url + 'b:%d' % Task.num_partitions

def reduce_output_stream1(stream, size, url, params):
    return 'fd', 'url:%d' % Task.num_partitions

def reduce_output_stream2(stream, size, url, params):
    assert stream == 'fd' and url == 'url:%d' % Task.num_partitions
    path, url = Task.reduce_output()
    return disco.fileutils.AtomicFile(path, 'w'), url.replace('disco://', 'foobar://')

class StreamsTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs           = ['apple', 'orange', 'pear']
    reduce_writer        = netstr_writer
    map_input_stream     = [map_input_stream, map_input_stream1, map_input_stream2]
    reduce_output_stream = [reduce_output_stream1, reduce_output_stream2]

    def getdata(self, path):
        return path

    @staticmethod
    def map_reader(stream, size, url):
        n = Task.num_partitions
        assert url == 'a:%db:%d' % (n, n)
        yield stream.read()

    @staticmethod
    def map(e, params):
        return [(e, '')]

    @staticmethod
    def reduce(iter, out, params):
        for k, v in iter:
            out.add('red:' + k, v)

    @property
    def results(self):
        def input_stream(stream, size, url, params):
            self.assert_(url.startswith('foobar://'))
            return stream, size, url.replace('foobar://', 'disco://')
        return result_iterator(self.job.wait(), input_stream=[input_stream, map_input_stream])

    def runTest(self):
        for k, v in self.results:
            self.assert_(k.startswith('red:ba'))
            self.assert_(k[6:] in self.inputs)
