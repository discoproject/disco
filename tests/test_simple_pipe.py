from disco.test import TestCase, TestPipe
from disco.compat import bytes_to_str
from disco.worker.pipeline.worker import Stage

def reduce_init(interface, params):
    return []

def reduce_done(interface, state):
    out = interface.output(0)
    for k, v in sorted(state):
        out.add(k, v)

class SimplePipe(TestPipe):
    def map(interface, state, label, inp):
        out = interface.output(0)
        for e in inp:
            out.add(int(e), (bytes_to_str(e)).strip())

    def reduce(interface, state, label, inp):
        for k, v in sorted(inp):
            state.append((k, v))

    pipeline = [("split", Stage("map", process=map)),
                ("group_all", Stage("reduce",
                                    init=reduce_init,
                                    process=reduce,
                                    done=reduce_done))]

class SimplerPipe(SimplePipe):
    def reduce_init(interface, params):
        return []

    def reduce(interface, state, label, inp):
        for rec in sorted(inp):
            state.append((int(rec), (bytes_to_str(rec).strip())))

    pipeline = [("group_all", Stage("reduce",
                                    init=reduce_init,
                                    process=reduce,
                                    done=reduce_done))]

class SimpleTestCase(TestCase):
    input = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31]

    def answers(self):
        return ((i, str(i)) for i in self.input for x in range(10))

    def serve(self, path):
        return '\n'.join([path] * 10)

    def test_simple(self):
        self.job = SimplePipe().run(input=self.test_server.urls(self.input))
        self.assertResults(self.job, self.answers())

    def test_simpler(self):
        self.job = SimplerPipe().run(input=self.test_server.urls(self.input))
        self.assertResults(self.job, self.answers())
