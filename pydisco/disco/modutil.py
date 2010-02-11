import re, struct, sys, os, imp, modulefinder
from os.path import abspath, dirname
from opcode import opname

from disco.error import ModUtilImportError

def user_paths():
    return set(os.getenv('PYTHONPATH', '').split(':') + [''])


def parse_function(function):
    code = function.func_code
    mod = re.compile(r'\x%.2x(..)\x%.2x' % (opname.index('LOAD_GLOBAL'),
                        opname.index('LOAD_ATTR')), re.DOTALL)
    return [code.co_names[struct.unpack('<H', x)[0]] for x in mod.findall(code.co_code)]

def recurse_module(module, path):
    finder = modulefinder.ModuleFinder(path=list(user_paths()))
    finder.run_script(path)
    return dict((name, module.__file__) for name, module in finder.modules.iteritems()
             if name != '__main__' and module.__file__)

def locate_modules(modules, recurse=True, include_sys=False):
    LOCALDIRS = user_paths()
    found = {}
    for module in modules:
        file, path, x = imp.find_module(module)
        if dirname(path) in LOCALDIRS:
            found[module] = path
            if recurse:
                found.update(recurse_module(module, path))
        elif include_sys:
            found[module] = None
    return found.items()


def find_modules(functions, send_modules=True, recurse=True, exclude=['Task']):
    modules = set()
    for function in functions:
        fmod = [m for m in parse_function(function) if m not in exclude]
        if send_modules:
            try:
                m = locate_modules(fmod, recurse, include_sys=True)
            except ImportError, e:
                raise ModUtilImportError(e, function)
            modules.update((k, v) if v else k for k, v in m)
        else:
            modules.update(fmod)
    return list(modules)
