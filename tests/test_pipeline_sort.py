import base64, string

from disco.test import TestCase, TestJob, TestPipe
from disco.util import kvgroup, shuffled
from disco.compat import bytes_to_str, str_to_bytes
from disco.worker.pipeline.worker import Stage
import time
import sys

alphanum = list(string.ascii_letters) + list(map(str, range(10)))

def Map(interface, state, label, inp):
    out = interface.output(0)
    for i in inp:
        for k, v in shuffled((base64.encodestring(str_to_bytes(c)), b'') for c in bytes_to_str(str(i) * 10)):
            out.add(k, v)

def Reduce(interface, state, label, inp):
    out = interface.output(0)
    for k, vs in kvgroup(inp):
        out.add((base64.decodestring(k)), len(list(vs)))

class SortJob(TestPipe):
    pipeline = [("split", Stage("Map", process=Map)),
                ("group_label", Stage("Reduce", process=Reduce, combine=True, sort=True))]

class SortTestCase(TestCase):
    def serve(self, path):
        return ''.join(alphanum)

    def runTest(self):
        self.job = SortJob().run(input=self.test_server.urls([''] * 100))
        result = [i for i in self.results(self.job)]
        self.assertResults(self.job, sorted((c, 1000) for c in alphanum))
