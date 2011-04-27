import __builtin__

from disco import util
from disco.settings import DiscoSettings
from discodb import DiscoDB, Q

def open(url, task=None):
    if task:
        job, args = task.jobobjs
        settings  = job.settings
    else:
        settings  = DiscoSettings()
    scheme, netloc, rest = util.urlsplit(url, settings)
    path, rest = rest.split('!', 1) if '!' in rest else (rest, '')
    discodb = DiscoDB.load(__builtin__.open(util.localize(path, settings)))

    if rest:
        method_name, arg = rest.split('/', 1) if '/' in rest else (rest, None)
        method = getattr(discodb, method_name)
        if method_name in ('metaquery', 'query'):
            return method(Q.urlscan(arg))
        return method(*filter(None, arg))
    return discodb

def input_stream(fd, size, url, params):
    return open(url, task=globals().get('Task')), size, url
