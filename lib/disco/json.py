# get rid of this for python2.6+
import imp, sys

def imp_path():
    return [path for path in sys.path if path != '.']

try:
    json = imp.load_module('json', *imp.find_module('json', imp_path()))
    loads, dumps = json.loads, json.dumps
except ImportError:
    try:
        from simplejson import loads, dumps
    except ImportError:
        from cjson import decode as loads
        from cjson import encode as dumps
