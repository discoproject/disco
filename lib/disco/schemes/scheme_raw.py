def input_stream(fd, sze, url, params):
    """Opens a StringIO whose data is everything after the url scheme.

    For example, `raw://hello_world` would return `hello_world` when read by the task.
    """
    from cStringIO import StringIO
    from disco.util import schemesplit
    scheme, string = schemesplit(url)
    ascii = string.encode('ascii')
    return (StringIO(ascii), len(ascii), url)

