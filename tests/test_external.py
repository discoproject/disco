from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.util import external

from subprocess import check_call, STDOUT
from os import uname, path

class ExternalTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs     = ["ape", "cat", "dog"]
    ext_params = {"test1": "1,2,3", "one two three": "dim\ndam\n", "dummy": "value"}
    sort       = False
    ext_map    = True

    def setUp(self):
        if uname()[0] == 'Darwin':
            self.skipTest('Cannot build static test_external on OS X')
        else:
            home = self.disco_settings['DISCO_HOME']
            check_call(['gcc', '-g', '-O3', '-static', '-Wall',
                        '-I', path.join(home, 'ext'),
                        '-o', path.join(home, 'tests', 'test_external'),
                        path.join(home, 'ext', 'disco.c'),
                        path.join(home, 'tests', 'test_external.c'),
                        '-l', 'Judy'],
                       stderr=STDOUT)
            self.map = external([path.join(home, 'tests', 'test_external')])
            super(ExternalTestCase, self).setUp()

    @staticmethod
    def reduce(iter, out, params):
        for k, v in iter:
            out.add("red_" + k, "red_" + v)

    def getdata(self, path):
        return 'test_%s\n' % path

    @property
    def answers(self):
        yield 'discoapi', ''

    def runTest(self):
        results = sorted((v, k) for k, v in self.results)
        for n, (v, k) in enumerate(results):
            self.assertEquals(k, 'red_dkey')
            self.assertEquals(v, 'red_test_%s\n' % self.inputs[n / 3])
        self.assertEquals(len(results), 9)
