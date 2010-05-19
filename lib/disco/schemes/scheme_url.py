import cStringIO

def input_stream(fd, size, url, params):
    return cStringIO.StringIO(url), len(url), url
