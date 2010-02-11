import cStringIO

def input_stream(fd, sze, url, params):
    """Opens a StringIO whose data is everything after the url scheme.

    For example, `raw://hello_world` would return `hello_world` when read by the task.
    """
    return (cStringIO.StringIO(url[6:]), len(url) - 6, url)

