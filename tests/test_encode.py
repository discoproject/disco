import base64, string

from disco.test import TestCase, TestJob, TestPipe
from disco.util import kvgroup, shuffled
from disco.compat import bytes_to_str, str_to_bytes
from disco.worker.pipeline.worker import Stage
import time
import sys

def Map(interface, state, label, inp):
    out = interface.output(0)
    for i in inp:
        out.add(str(i), u'\x00\x00')

def Reduce(interface, state, label, inp):
    out = interface.output(0)
    for k, vs in kvgroup(inp):
        out.add(str(k), 0)

class SortJob(TestPipe):
    pipeline = [("split", Stage("Map", process=Map)),
                ("group_label", Stage("Reduce", process=Reduce, combine=True, sort=True))]

class SortTestCase(TestCase):
    def serve(self, path):
        return 'a'

    def runTest(self):
        self.job = SortJob().run(input=self.test_server.urls([''] * 10))
        result = [i for i in self.results(self.job)]
        self.assertResults(self.job, [('a', 0)])
