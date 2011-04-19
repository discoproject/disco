def input_stream(fd, size, url, params):
    from disco import util
    from discodb import DiscoDB, Q
    scheme, netloc, rest = util.urlsplit(url, localhost=True)
    path, rest = rest.split('!', 1) if '!' in rest else (rest, '')
    discodb = DiscoDB.load(open(path))

    if rest:
        method_name, arg = rest.split('/', 1) if '/' in rest else (rest, None)
        method = getattr(discodb, method_name)
        if method_name in ('metaquery', 'query'):
            return method(Q.urlscan(arg)), size, url
        return method(*filter(None, arg)), size, url
    return discodb, size, url
