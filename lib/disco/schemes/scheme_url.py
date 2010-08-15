def input_stream(fd, size, url, params):
    import cStringIO
    return cStringIO.StringIO(url), len(url), url
