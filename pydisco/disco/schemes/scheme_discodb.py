import os
from disco import comm, core, util
from discodb import DiscoDB, Q

class NotMethod(Exception):
    def __init__(self, method):
        self.method = method

def maybe_method(datadir, rest, method, xargs=None):
    if rest.find('/%s/' % method) > 0:
        file, arg = rest.split('/%s/' % method, 1)
        path = os.path.join(datadir, file)
        if os.path.isfile(path):
            bound_method = getattr(DiscoDB.load(open(path)), method)
            return bound_method(xargs(arg)) if xargs else bound_method()
    raise NotMethod(method)

def input_stream(fd, size, url, params):
    scheme, rest = url.split('://', 1)
    host, rest = rest.split('/', 1)

    if hasattr(params, "discodb_query"):
        query = lambda x: params.discodb_query
    else:
        query = Q.urlscan

    if host == Task.host or Task.has_flag("resultfs"):
        datadir = os.path.join(Task.root, "data")
        try:
            return maybe_method(datadir, rest, 'query', xargs=query), size, params
        except NotMethod, e:
            pass
        for method in ('keys', 'values'):
            try:
                return maybe_method(datadir, rest, method), size, params
            except NotMethod, e:
                pass
        return DiscoDB.load(open(os.path.join(datadir, rest))), size, params
    raise core.DiscoError("Scheme 'discodb' can only be used with force_local=True")

