from disco import modutil
from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.error import ModUtilImportError

import sys, os

def system_modules():
    os.path.abspath('')
    random.random()
    time.time()

def local_module():
    extramodule1.magic(5)

def missing_module():
    missing_module_ifitexiststhistestwillfail.magic()

def recursive_module():
    mod1.plusceil(1, 2)

class ModUtilLocalTestCase(DiscoTestCase):
    def setUp(self):
        self.sys_path = sys.path
        self.support  = os.path.join(os.path.dirname(__file__), 'support')
        sys.path.append(self.support)
        os.environ['PYTHONPATH'] += ':%s' % self.support

    def tearDown(self):
        sys.path = self.sys_path

    def assertFindsModules(self, functions, modules, send_modules=False, recurse=True):
        self.assertEquals(sorted(modutil.find_modules(functions,
                                                      send_modules=send_modules,
                                                      recurse=recurse)),
                          sorted(modules))

    def test_system(self):
        self.assertFindsModules([system_modules], ['os', 'random', 'time'])

    def test_local(self):
        self.assertFindsModules([local_module], ['extramodule1'])

    def test_missing(self):
        self.assertRaises(ModUtilImportError, lambda: modutil.find_modules([missing_module]))

    def test_recursive(self):
        self.assertFindsModules([recursive_module],
                                [('mod1', os.path.normpath(os.path.join(self.support, 'mod1.py'))),
                                 ('mod2', os.path.normpath(os.path.join(self.support, 'mod2.py')))],
                                send_modules=True)

    def test_norecursive(self):
        self.assertFindsModules([recursive_module], ['mod1'], recurse=False)

class ModUtilTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = ['0.5|1.2']

    def getdata(self, path):
        return '%s\n' % path

    @staticmethod
    def map(e, params):
        x, y = [float(x) for x in e.split('|')]
        return [(mod1.plusceil(x, y) + math.ceil(1.5), '')]

    @property
    def answers(self):
        yield 4.0, ''

    def setUp(self):
        self.sys_path = sys.path
        self.support  = os.path.join(os.path.dirname(__file__), 'support')
        sys.path.append(self.support)
        os.environ['PYTHONPATH'] += ':%s' % self.support
        super(ModUtilTestCase, self).setUp()

    def tearDown(self):
        sys.path = self.sys_path
        super(ModUtilTestCase, self).tearDown()

class AnotherModUtilTestCase(ModUtilTestCase):
    @property
    def required_modules(self):
        return modutil.find_modules([self.map])

class YetAnotherModUtilTestCase(ModUtilTestCase):
    required_modules = ['math', 'mod1', 'mod2']
