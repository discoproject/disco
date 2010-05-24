from disco import comm

def input_stream(fd, sze, url, params):
    """Opens the specified url using an http client."""
    return comm.open_remote(url)
