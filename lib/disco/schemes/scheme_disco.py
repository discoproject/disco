from disco import comm, util
from disco.settings import DiscoSettings

def open(url, task=None):
    if task:
        localhost = task.host
        job, args = task.jobobjs
        settings  = job.settings
    else:
        localhost = None
        settings  = DiscoSettings()
    scheme, netloc, path = util.urlsplit(url, localhost, settings)
    return comm.open_url(util.urljoin((scheme, netloc, path)))

def input_stream(fd, size, url, params):
    """
    Opens the path on host locally if it is local, otherwise over http.
    """
    file = open(url, task=globals().get('Task'))
    return file, len(file), file.url
