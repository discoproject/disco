import os
from disco.comm import open_local

def input_stream(fd, size, url, params):
    """Opens the url path locally, relative to the path `[Task.root]/input`."""
    t, fname = url[6:].split("/", 1)
    path = os.path.join(Task.root, "input", fname)
    return open_local(path, url)

