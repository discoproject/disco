import glob, unittest
from inspect import getmembers, isclass

loader = unittest.TestLoader()

for modname in (n[:-3] for n in glob.glob('test_*.py')):
    for suite in loader.loadTestsFromName(modname):
        if suite.countTestCases():
            cls = list(suite)[0].__class__
            tests = loader.getTestCaseNames(cls)
            if tests:
                for n in tests:
                    print('{0}.{1}.{2}'.format(modname, cls.__name__, n))
            else:
                print('{0}.{1}'.format(modname, cls.__name__))
