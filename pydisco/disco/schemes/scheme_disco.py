import os
from disco import comm

def input_stream(fd, size, url, params):
    """Opens the path on host using an http client and the setting `DISCO_PORT`.

    For instance, if `DISCO_PORT = 8989`, `disco://host/path` would be converted to `http://host:8989/path`.
    """
    host, fname = url[8:].split("/", 1)
    if host == Task.host or Task.has_flag("resultfs"):
        path = os.path.join(Task.root, "data", fname)
        return comm.open_local(path, url)
    return comm.open_remote("http://%s:%s/%s" % (host, Task.port, fname))
