from disco.test import TestCase, TestJob
from disco.func import map_input_stream, reduce_output_stream, disco_output_stream

def map_input_stream1(stream, size, url, params):
    return cStringIO.StringIO('a' + stream.read())

def map_input_stream2(stream, size, url, params):
    return cStringIO.StringIO('b' + stream.read())

class StreamsJob(TestJob):
    map_input_stream = [map_input_stream,
                        map_input_stream1,
                        map_input_stream2]
    reduce_output_stream = [reduce_output_stream, disco_output_stream]

    @staticmethod
    def map_reader(stream, size, url):
        yield stream.read()

    @staticmethod
    def map(e, params):
        yield e, ''

    @staticmethod
    def reduce(iter, params):
        for k, v in sorted(iter):
            yield 'red:' + k, v

class StreamsTestCase(TestCase):
    def serve(self, path):
        return path

    def runTest(self):
        input = ['apple', 'orange', 'pear']
        self.job = StreamsJob().run(input=self.test_server.urls(input))
        self.assertResults(self.job, (('red:ba%s' % i, '') for i in input))
