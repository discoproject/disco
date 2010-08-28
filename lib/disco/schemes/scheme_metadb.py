def input_stream(fd, size, url, params):
    import os
    from disco import util
    from disco.comm import download
    from discodb import MetaDB, Q
    scheme, netloc, rest = util.urlsplit(url)
    path, rest = rest.split('!', 1) if '!' in rest else (rest, '')

    if netloc[0] == Task.netloc[0]:
        discodb = MetaDB.load(os.path.join(Task.root, path))
    else:
        discodb = MetaDB.loads(download('disco://%s/%s' % (netloc, path)))

    if rest:
        method, arg = rest.split('/', 1)
        if method in ('metadb', 'datadb'):
            discodb = getattr(discodb, method)
            method, arg = arg.split('/', 1)
        if method == 'query':
            if hasattr(params, 'discodb_query'):
                return discodb.query(params.discodb_query), size, url
            return discodb.query(Q.urlscan(arg)), size, url
        return getattr(discodb, method)(), size, url
    return discodb, size, url
