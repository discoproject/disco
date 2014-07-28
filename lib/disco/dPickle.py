import functools, marshal, pickle, types, sys
from disco.compat import pickle_loads, BytesIO
from inspect import getfile, getmodule
from os.path import dirname

loads = pickle_loads

def dumps(obj, protocol=None):
    file = BytesIO()
    Pickler(file, protocol).dump(obj)
    return file.getvalue()

def is_std(module, stdlib=(dirname(getfile(pickle)),)):
    return module.__name__ != '__main__' and dirname(getfile(module)) in stdlib

def unfunc(packed, globals={'__builtins__': __builtins__}):
    code, defs = marshal.loads(packed)
    return types.FunctionType(code, globals, argdefs=defs)

def unpartial(packed):
    func, args, kwds = loads(packed)
    return functools.partial(func, *args, **kwds)

if sys.version_info[0] == 3:
    cls = pickle._Pickler
else:
    cls = pickle.Pickler

if sys.version_info[0:2] == (2,6):
    class Pickler(cls):
        dispatch = cls.dispatch.copy()

        def save_func(self, func):
            if is_std(getmodule(func)) or func.__module__.startswith('disco.'):
                self.save_global(func)
            else:
                packed = marshal.dumps((func.__code__, func.__defaults__))
                self.save_reduce(unfunc, (packed,), obj=func)
        dispatch[types.FunctionType] = save_func

        def save_partial(self, partial):
            packed = dumps((partial.func, partial.args, partial.keywords or {}))
            self.save_reduce(unpartial, (packed,), obj=partial)
        dispatch[functools.partial] = save_partial
else:
    class Pickler(cls):
        dispatch = cls.dispatch.copy()

        def save_func(self, func):
            if is_std(getmodule(func)) or func.__module__.startswith('disco.'):
                self.save_global(func)
            else:
                packed = marshal.dumps((func.__code__, func.__defaults__))
                self.save_reduce(unfunc, (packed,), obj=func)
        dispatch[types.FunctionType] = save_func
