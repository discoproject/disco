def input_stream(fd, size, url, params):
    """
    Opens the path on host locally if it is local, otherwise over http.
    """
    return Task.open_url(url)
