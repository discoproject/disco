def input_stream(fd, size, url, params):
    """Opens the url path locally, relative to the path `[Task.root]/input`."""
    import os
    from disco.comm import open_local
    from disco.settings import DiscoSettings
    t, fname = url[6:].split("/", 1)
    path = os.path.join(DiscoSettings()['DISCO_ROOT'], "input", fname)
    return open_local(path)

