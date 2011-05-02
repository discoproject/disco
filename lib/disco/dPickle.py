import cPickle, cStringIO, functools, marshal, pickle, types

from cPickle import loads

def dumps(obj, protocol=None):
    file = cStringIO.StringIO()
    Pickler(file, protocol).dump(obj)
    return file.getvalue()

def pack(object):
    if hasattr(object, 'func_code'):
        if object.func_closure != None:
            raise TypeError("Function must not have closures: "
                            "%s (try using functools.partial instead)"
                            % object.func_name)
        return marshal.dumps((object.func_code, object.func_defaults))
    if isinstance(object, (list, tuple)):
        object = type(object)(pack(o) for o in object)
    return cPickle.dumps(object, cPickle.HIGHEST_PROTOCOL)

def unpack(string, globals={'__builtins__': __builtins__}):
    try:
        object = cPickle.loads(string)
        if isinstance(object, (list, tuple)):
            return type(object)(unpack(s, globals=globals) for s in object)
        return object
    except Exception:
        try:
            code, defs = marshal.loads(string)
            return types.FunctionType(code, globals, argdefs=defs)
        except Exception, e:
            raise ValueError("Could not unpack: %s (%s)" % (string, e))

def unpartial(string):
    func, args, kwds = unpack(string)
    return functools.partial(func, *args, **kwds)

class Pickler(pickle.Pickler):
    dispatch = pickle.Pickler.dispatch.copy()

    def save_func(self, func):
        if func.__module__.startswith('disco.'):
            self.save_global(func)
        else:
            self.save_reduce(unpack, (pack(func),), obj=func)
    dispatch[types.FunctionType] = save_func

    def save_partial(self, partial):
        parts = partial.func, partial.args, partial.keywords or {}
        self.save_reduce(unpartial, (pack(parts),), obj=partial)
    dispatch[functools.partial] = save_partial

