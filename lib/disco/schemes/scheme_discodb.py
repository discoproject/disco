def input_stream(fd, size, url, params):
    import os
    from disco import util
    from disco.settings import DiscoSettings
    from discodb import DiscoDB, Q
    scheme, netloc, rest = util.urlsplit(url)
    path, rest = rest.split('!', 1) if '!' in rest else (rest, '')
    root = DiscoSettings()['DISCO_ROOT']
    discodb = DiscoDB.load(open(os.path.join(root, path)))

    if rest:
        method_name, arg = rest.split('/', 1) if '/' in rest else (rest, None)
        method = getattr(discodb, method_name)
        if method_name in ('metaquery', 'query'):
            return method(Q.urlscan(arg)), size, url
        return method(*filter(None, arg)), size, url
    return discodb, size, url
