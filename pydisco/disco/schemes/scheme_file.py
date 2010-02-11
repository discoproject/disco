from disco.comm import open_local

def input_stream(fd, size, url, params):
    """Opens the url locally on the node."""
    return open_local(url[7:], url)

