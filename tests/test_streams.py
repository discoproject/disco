from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.core import result_iterator
from disco.func import map_input_stream, reduce_output_stream, disco_output_stream

def map_input_stream1(stream, size, url, params):
    return cStringIO.StringIO('a' + stream.read())

def map_input_stream2(stream, size, url, params):
    return cStringIO.StringIO('b' + stream.read())

class StreamsTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = ['apple', 'orange', 'pear']
    map_input_stream = [map_input_stream,
                        map_input_stream1,
                        map_input_stream2]
    reduce_output_stream = [reduce_output_stream, disco_output_stream]

    def getdata(self, path):
        return path

    @staticmethod
    def map_reader(stream, size, url):
        yield stream.read()

    @staticmethod
    def map(e, params):
        return [(e, '')]

    @staticmethod
    def reduce(iter, params):
        for k, v in sorted(iter):
            yield 'red:' + k, v

    @property
    def answers(self):
        for i in self.inputs:
            yield 'red:ba%s' % i, ''
