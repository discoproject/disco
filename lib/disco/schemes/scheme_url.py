def input_stream(fd, size, url, params):
    from disco.compat import StringIO
    return StringIO(url), len(url), url
