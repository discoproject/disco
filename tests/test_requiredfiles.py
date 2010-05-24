from disco.test import DiscoJobTestFixture, DiscoTestCase

import os

class RequiredFilesTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs       = ['123']
    required_modules = ['extramodule1', 'extramodule2']

    @property
    def required_files(self):
        return [os.path.join(self.disco_settings['DISCO_HOME'], 'tests', 'support', module)
            for module in ('extramodule1.py', 'extramodule2.py')]

    def getdata(self, path):
        return '%s\n' % path

    @staticmethod
    def map(e, params):
        x = extramodule1.magic(int(e))
        y = extramodule2.kungfu(x)
        return [('', y)]

    @property
    def answers(self):
        yield '', int(self.inputs[0]) ** 2 + 2
