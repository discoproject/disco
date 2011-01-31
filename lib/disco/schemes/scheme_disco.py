def input_stream(fd, size, url, params):
    """
    Opens the path on host locally if it is local, otherwise over http.
    """
    from disco.comm import open_url
    return open_url(url)
