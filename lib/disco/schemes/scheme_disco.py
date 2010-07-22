import os
from disco import comm
from disco.util import urlsplit

def input_stream(fd, size, url, params):
    """
    Opens the path on host using an http client and the setting `DISCO_PORT`.
    """
    scheme, netloc, rest = urlsplit(url)
    prefix, fname = rest.split('/', 1)
    if netloc[0] == Task.netloc[0]:
        if prefix == 'ddfs':
            root = Task.settings['DDFS_ROOT']
        else:
            root = Task.settings['DISCO_DATA']
        path = os.path.join(root, fname)
        return comm.open_local(path)
    return comm.open_remote('http://%s/%s/%s' % (netloc, prefix, fname))
