# get rid of this for python2.6+
import imp, sys, os

def imp_path():
    cwd = os.path.realpath('.')
    return [path for path in sys.path if os.path.realpath(path) != cwd]

try:
    json = imp.load_module('json', *imp.find_module('json', imp_path()))
    loads, dumps = json.loads, json.dumps
except ImportError:
    try:
        from simplejson import loads, dumps
    except ImportError:
        from cjson import decode as loads
        from cjson import encode
        def dumps(x):
            # do the same thing as simplejson: assume that all strings are utf-8
            if isinstance(x, str):
                x = x.decode('utf-8')
            return encode(x)
