def input_stream(fd, size, url, params):
    """Opens the url locally on the node."""
    from disco.comm import open_url
    return open_url(url)

