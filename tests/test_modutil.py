from disco import modutil
from disco.test import TestCase, TestJob
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

class ModUtilJob(TestJob):
    @staticmethod
    def map(e, params):
        x, y = [float(x) for x in e.split('|')]
        yield mod1.plusceil(x, y) + math.ceil(1.5), ''

class ModUtilTestCase(TestCase):
    def serve(self, path):
        return '%s\n' % path

    def setUp(self):
        super(ModUtilTestCase, self).setUp()
        self.sys_path = sys.path
        self.support  = os.path.join(os.path.dirname(__file__), 'support')
        sys.path.append(self.support)
        os.environ['PYTHONPATH'] += ':%s' % self.support

    def tearDown(self):
        super(ModUtilTestCase, self).tearDown()
        sys.path = self.sys_path

    def assertFindsModules(self, functions, modules, send_modules=False, recurse=True):
        self.assertEquals(sorted(modutil.find_modules(functions,
                                                      send_modules=send_modules,
                                                      recurse=recurse)),
                          sorted(['test_modutil'] + modules))

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

    def test_auto_modules(self):
        self.job = ModUtilJob().run(input=self.test_server.urls(['0.5|1.2']))
        self.assertResults(self.job, [(4.0, '')])

    def test_find_modules(self):
        self.job = ModUtilJob()
        self.job.run(input=self.test_server.urls(['0.5|1.2']),
                     required_modules=modutil.find_modules([self.job.map]))
        self.assertResults(self.job, [(4.0, '')])

    def test_list_modules(self):
        reqs = ['math',
                ('mod1', os.path.normpath(os.path.join(self.support, 'mod1.py'))),
                ('mod2', os.path.normpath(os.path.join(self.support, 'mod2.py')))]
        self.job = ModUtilJob().run(input=self.test_server.urls(['0.5|1.2']),
                                    required_modules=reqs)
        self.assertResults(self.job, [(4.0, '')])
