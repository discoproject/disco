import os
from disco import comm, core, util
from discodb import DiscoDB, Q

def input_stream(fd, size, url, params):
    scheme, netloc, rest = util.urlsplit(url)

    if netloc == Task.netloc:
        path, rest   = rest.split('!', 1) if '!' in rest else (rest, '')
        Task.discodb = DiscoDB.load(open(os.path.join(Task.root, path)))

        if rest:
            method, arg = rest.split('/', 1)
            if method == 'query':
                if hasattr(params, 'discodb_query'):
                    return Task.discodb.query(params.discodb_query), size, url
                return Task.discodb.query(Q.urlscan(arg)), size, url
            return getattr(Task.discodb, method)(), size, url
        return Task.discodb, size, url
    raise core.DiscoError("Scheme 'discodb' can only be used with force_local=True")
