from disco.test import TestCase, TestPipe
from disco.worker.pipeline.worker import Stage

def map(interface, state, label, inp):
    out = interface.output(0)
    for e in inp:
        out.add(e, '')

class RawJob(TestPipe):
    pipeline = [("split", Stage("map", process=map))]

class RawTestCase(TestCase):
    input = ['raw://eeny', 'raw://meeny', 'raw://miny', 'raw://moe']

    def serve(self, path):
        return path

    def runTest(self):
        self.job = RawJob().run(input=self.test_server.urls(self.input))
        self.assertEqual(sorted(self.results(self.job)),
                         sorted((i, '') for i in self.input))

