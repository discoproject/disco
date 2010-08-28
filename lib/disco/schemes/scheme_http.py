def input_stream(fd, sze, url, params):
    """Opens the specified url using an http client."""
    from disco import comm
    return comm.open_remote(url)
