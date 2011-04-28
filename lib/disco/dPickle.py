import functools, marshal, pickle, types

from cPickle import loads
from cStringIO import StringIO

def dumps(obj, protocol=None):
    file = StringIO()
    Pickler(file, protocol).dump(obj)
    return file.getvalue()

def unfunc(packed, globals={'__builtins__': __builtins__}):
    code, defs = marshal.loads(packed)
    return types.FunctionType(code, globals, argdefs=defs)

def unpartial(packed):
    func, args, kwds = loads(packed)
    return functools.partial(func, *args, **kwds)

class Pickler(pickle.Pickler):
    dispatch = pickle.Pickler.dispatch.copy()

    def save_func(self, func):
        if func.__module__.startswith('disco.'):
            self.save_global(func)
        else:
            packed = marshal.dumps((func.func_code, func.func_defaults))
            self.save_reduce(unfunc, (packed,), obj=func)
    dispatch[types.FunctionType] = save_func

    def save_partial(self, partial):
        packed = dumps((partial.func, partial.args, partial.keywords or {}))
        self.save_reduce(unpartial, (packed,), obj=partial)
    dispatch[functools.partial] = save_partial

