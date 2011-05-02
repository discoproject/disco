from disco import comm, util
from disco.settings import DiscoSettings

def open(url, task=None):
    if task:
        scheme, netloc, path = util.urlsplit(url,
                                             localhost=task.host,
                                             disco_port=task.disco_port,
                                             disco_data=task.disco_data,
                                             ddfs_data=task.ddfs_data)
    else:
        scheme, netloc, path = util.urlsplit(url, localhost=None)
    return comm.open_url(util.urljoin((scheme, netloc, path)))

def input_stream(fd, size, url, params):
    """
    Opens the path on host locally if it is local, otherwise over http.
    """
    file = open(url, task=globals().get('Task'))
    return file, len(file), file.url
