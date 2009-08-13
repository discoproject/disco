import re, struct, os, imp, modulefinder
from os.path import abspath, dirname
from opcode import opname


def parse_function(fun):
        code = fun.func_code
        mod = re.compile("\\x%.2x(..)\\x%.2x" % (opname.index("LOAD_GLOBAL"),\
                opname.index("LOAD_ATTR")), re.DOTALL)
        modules = []
        for x in mod.findall(code.co_code):
                modules.append(code.co_names[struct.unpack("<H", x)[0]])
        return modules

def recurse_module(mod, path):
        LOCALDIRS = [abspath(x)\
                for x in os.environ.get("PYTHONPATH", "").split(":") + [""]]
        found = {}
        finder = modulefinder.ModuleFinder(path = LOCALDIRS)
        finder.run_script(path)
        for p in finder.modules.values():
                if p.__file__ and p.__name__ != "__main__" and\
                        dirname(p.__file__) in LOCALDIRS:
                        found[p.__name__] = p.__file__
        return found 

def locate_modules(modules, recurse = True, include_sys = False):
        LOCALDIRS = [abspath(x)\
                for x in os.environ.get("PYTHONPATH", "").split(":") + [""]]
        sys = {}
        found = {}
        for mod in modules:
                try:
                        mode, path, x = imp.find_module(mod)
                except ImportError, x:
                        x.module = mod
                        raise x
                if not mode:
                        continue
                if dirname(path) in LOCALDIRS:
                        found[mod] = path
                        if recurse:
                                e = recurse_module(mod, path)
                                found.update(e)
                elif include_sys:
                        sys[mod] = None

        return found.items() + sys.items()


def find_modules(funs, send_modules = True, recurse = True):
        mods = {}
        for fun in funs:
                fmod = parse_function(fun)
                if send_modules:
                        try:
                                m = locate_modules(fmod, recurse,\
                                        include_sys = True)
                                mods.update(m)
                        except ImportError, x:
                                raise ImportError("Could not find module %s "\
                                "defined in %s. Maybe it is a typo. If it is "\
                                "a valid module, see documetation of the "\
                                "required_modules parameter for instructions "\
                                "on how to include it properly." %\
                                        (x.module, fun.func_name))
                else:
                        mods.update((m, None) for m in fmod)
        if send_modules:
                return [(k, v) for k, v in mods.iteritems() if v] +\
                       [k for k, v in mods.iteritems() if not v]
        else:
                return mods.keys()
        

