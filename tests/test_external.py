from disco.test import TestCase, TestJob
from disco.util import external

from subprocess import check_call, STDOUT
from os import uname, path

class ExternalJob(TestJob):
    ext_params = {"test1": "1,2,3", "one two three": "dim\ndam\n", "dummy": "value"}
    sort = False

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            yield "red_" + k, "red_" + v

class ExternalTestCase(TestCase):
    inputs = ['ape', 'cat', 'dog']

    def serve(self, path):
        return 'test_%s\n' % path

    def runTest(self):
        if uname()[0] == 'Darwin':
            self.skipTest('Cannot build static test_external on OS X')
        else:
            home = self.settings['DISCO_HOME']
            test = path.join(home, 'tests', 'test_external')
            check_call(['gcc', '-g', '-O3', '-static', '-Wall',
                        '-I', path.join(home, 'ext'),
                        '-o', test,
                        path.join(home, 'ext', 'disco.c'),
                        path.join(home, 'tests', 'test_external.c'),
                        '-l', 'Judy'],
                       stderr=STDOUT)
            self.job = ExternalJob().run(input=self.test_server.urls(self.inputs),
                                         map=external([test]))
            results = sorted((v, k) for k, v in self.results(self.job))
            for n, (v, k) in enumerate(results):
                self.assertEquals(k, 'red_dkey')
                self.assertEquals(v, 'red_test_%s\n' % self.inputs[n / 3])
            self.assertEquals(len(results), 9)
