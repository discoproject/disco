from disco.compat import StringIO
from disco.test import TestCase, TestJob
from disco.worker.task_io import disco_output_stream
from disco.worker.classic.func import map_input_stream, reduce_output_stream
from disco.compat import bytes_to_str

def map_input_stream1(stream, size, url, params):
    return StringIO('a' + bytes_to_str(stream.read()))

def map_input_stream2(stream, size, url, params):
    return StringIO('b' + bytes_to_str(stream.read()))

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
        self.assertResults(self.job, (('red:ba{0}'.format(i), '') for i in input))
