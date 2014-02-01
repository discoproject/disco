from disco.test import TestCase, TestPipe
from disco.compat import bytes_to_str
from disco.worker.pipeline.worker import Stage

def reduce_init(interface, params):
    return []

def reduce_done(interface, state):
    out = interface.output(0)
    for k, v in sorted(state):
        out.add(k, v)

def map(interface, state, label, inp):
    out = interface.output(0)
    for e in inp:
        out.add(int(e), (bytes_to_str(e)).strip())

def intermediate(interface, state, label, inp):
    out = interface.output(0)
    for k, v in sorted(inp):
        out.add(k, v)

def reduce(interface, state, label, inp):
    for k, v in sorted(inp):
        state.append((k, v))

def getPipeline(count, type):
    intermediates = [(type, Stage("inter_%d" % i, process=intermediate)) for i in range(count)]
    pipeline = [("split", Stage("map", process=map))] + intermediates + [("group_label", Stage("reduce",
                                                                          init=reduce_init,
                                                                          process=reduce, done=reduce_done))]
    return pipeline

class PipePerNodeJob(TestPipe):
    pipeline = getPipeline(10, "group_node_label")

class PipeGlobalJob(TestPipe):
    pipeline = getPipeline(10, "group_label")

class SimpleTestCase(TestCase):
    input = range(1000)

    def answers(self):
        return ((i, str(i)) for i in self.input for x in range(10000))

    def serve(self, path):
        return '\n'.join([path] * 10000)

    def test_per_node(self):
        self.job = PipePerNodeJob().run(input=self.test_server.urls(self.input))
        self.assertResults(self.job, self.answers())

    def test_global(self):
        self.job = PipeGlobalJob().run(input=self.test_server.urls(self.input))
        self.assertResults(self.job, self.answers())
