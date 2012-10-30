def input_stream(fd, sze, url, params):
    """Opens a StringIO whose data is everything after the url scheme.

    For example, `raw://hello_world` would return `hello_world` when read by the task.
    """
    from disco.compat import StringIO, bytes_to_str
    from disco.util import schemesplit
    scheme, string = schemesplit(url)
    ascii = bytes_to_str(string)
    return (StringIO(ascii), len(ascii), url)

