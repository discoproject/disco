import os
from disco import comm

def input_stream(fd, size, url, params):
    """
    Opens the path on host using an http client and the setting `DISCO_PORT`.
    """
    host, prefix, fname = url[8:].split("/", 2)
    if host == Task.host or Task.has_flag("resultfs"):
        if prefix == "ddfs":
            root = Task.ddfsroot
        else:
            root = Task.dataroot
        path = os.path.join(root, fname)
        return comm.open_local(path, url)
    return comm.open_remote("http://%s:%s/%s/%s" %\
                            (host, Task.port, prefix, fname))
