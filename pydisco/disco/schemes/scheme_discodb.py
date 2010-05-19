import os
from disco import comm, core, util
from discodb import DiscoDB, Q

def input_stream(fd, size, url, params):
    scheme, host, rest = util.urlsplit(url)

    if host == Task.host or Task.has_flag('resultfs'):
        host, dir, jobname, rest = rest.split('/', 3)
        file, rest   = rest.split('/', 1) if '/' in rest else (rest, '')
        path         = os.path.join(Task.datadir, host, dir, jobname, file)
        Task.discodb = DiscoDB.load(open(path))

        if rest:
            method, arg = rest.split('/', 1)
            if method == 'query':
                if hasattr(params, 'discodb_query'):
                    return Task.discodb.query(params.discodb_query), size, url
                return Task.discodb.query(Q.urlscan(arg)), size, url
            return getattr(Task.discodb, method)(), size, url
        return Task.discodb, size, url
    raise core.DiscoError("Scheme 'discodb' can only be used with force_local=True")
