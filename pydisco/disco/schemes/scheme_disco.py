import os
from disco import comm

def input_stream(fd, size, url, params):
        host, fname = url[8:].split("/", 1)
        if host == Task.host or Task.has_flag("resultfs"):
                path = os.path.join(Task.root, "data", fname)
                return comm.open_local(path, url)
        else:
                return comm.open_remote("http://%s:%s/%s" % (host, Task.port, fname))
