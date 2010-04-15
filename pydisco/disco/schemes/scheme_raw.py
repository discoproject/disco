from cStringIO import StringIO
from disco.util import schemesplit

def input_stream(fd, sze, url, params):
    """Opens a StringIO whose data is everything after the url scheme.

    For example, `raw://hello_world` would return `hello_world` when read by the task.
    """
    scheme, string = schemesplit(url)
    return (StringIO(string), len(string), url)

