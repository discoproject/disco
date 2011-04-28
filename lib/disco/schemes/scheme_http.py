from disco import comm

def open(url, task=None):
    return comm.open_url(url)

def input_stream(fd, sze, url, params):
    """Opens the specified url using an http client."""
    file = open(url, task=globals().get('Task'))
    return file, len(file), file.url
