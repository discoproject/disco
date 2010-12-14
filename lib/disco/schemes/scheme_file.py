def input_stream(fd, size, url, params):
    """Opens the url locally on the node."""
    from disco.comm import open_local
    from disco.util import schemesplit
    scheme, path = schemesplit(url)
    return open_local(path)

