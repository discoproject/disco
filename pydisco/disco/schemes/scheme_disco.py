import os
from disco import comm
from disco.util import urlsplit

def input_stream(fd, size, url, params):
    """
    Opens the path on host using an http client and the setting `DISCO_PORT`.
    """
    scheme, netloc, rest = urlsplit(url)
    prefix, fname = rest.split('/', 1)
    if netloc == Task.netloc:
        if prefix == 'ddfs':
            root = Task.ddfsroot
        else:
            root = Task.dataroot
        path = os.path.join(root, fname)
        return comm.open_local(path, url)
    return comm.open_remote('http://%s/%s/%s' % (netloc, prefix, fname))
