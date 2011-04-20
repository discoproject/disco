from disco.test import TestCase, TestJob
from disco.util import external

from subprocess import check_call, STDOUT
from os import uname, path

class ExternalJob(TestJob):
    ext_params = {"test1": "1,2,3", "one two three": "dim\ndam\n", "dummy": "value"}
    sort = False

class ExternalTestCase(TestCase):
    inputs = ['ape', 'cat', 'dog']

    def serve(self, path):
        return 'test_%s\n' % path

    def setUp(self):
        super(ExternalTestCase, self).setUp()
        if uname()[0] == 'Darwin':
            self.skipTest('Cannot build static test_external on OS X')
        else:
            home = self.settings['DISCO_HOME']
            self.binary = path.join(home, 'tests', 'test_external')
            check_call(['gcc', '-g', '-O3', '-static', '-Wall',
                        '-I', path.join(home, 'ext'),
                        '-o', self.binary,
                        path.join(home, 'ext', 'disco.c'),
                        path.join(home, 'tests', 'test_external.c'),
                        '-l', 'Judy'],
                       stderr=STDOUT)

    def test_extmap(self):
        def reduce(iter, params):
            for k, v in iter:
                yield "red_" + k, "red_" + v
        self.job = ExternalJob().run(input=self.test_server.urls(self.inputs),
                                     map=external([self.binary]),
                                     reduce=reduce)
        results = sorted((v, k) for k, v in self.results(self.job))
        for n, (v, k) in enumerate(results):
            self.assertEquals(k, 'red_dkey')
            self.assertEquals(v, 'red_test_%s\n' % self.inputs[n / 3])
        self.assertEquals(len(results), 9)

    def test_extreduce(self):
        self.job = ExternalJob().run(input=self.test_server.urls(self.inputs),
                                     map=lambda e, params: [('', e)],
                                     reduce=external([self.binary]))
        ans = str(sum(map(ord, ''.join('test_%s\n' % i for i in self.inputs))))
        self.assertEquals([(ans, ans)] * 10, list(self.results(self.job)))
