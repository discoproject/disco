import sys
from types import FunctionType
from core import Disco, Params, result_iterator
import func

class DeprecatedFunc:
        def __init__(self, fun, name):
                self.fun = fun
                self.name = name

        def __getattr__(self, name):
                if name == "func_code":
                        print >> sys.stderr, "WARNING! disco.%s is "\
                                "deprecated. Use disco.func.%s instead."\
                                        % (name, name)
                        return fun.func_code
                raise AttributeException("%s not found" % name)

chain_reader = DeprecatedFunc(func.chain_reader, "chain_reader")

def job(master, name, input, fun_map = None, **kw):
        kw.update({"name": name, "input": input})
        kw["map"] = kw.get("map", fun_map)
        return Disco(master).new_job(**kw).wait(clean = kw.get("clean", True))
