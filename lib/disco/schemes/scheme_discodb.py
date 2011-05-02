import __builtin__

from disco import util
from discodb import DiscoDB, Q

def open(url, task=None):
    if task:
        disco_data = task.disco_data
        ddfs_data = task.ddfs_data
    else:
        from disco.settings import DiscoSettings
        settings = DiscoSettings()
        disco_data = settings['DISCO_DATA']
        ddfs_data = settings['DDFS_DATA']
    scheme, netloc, rest = util.urlsplit(url)
    path, rest = rest.split('!', 1) if '!' in rest else (rest, '')
    discodb = DiscoDB.load(__builtin__.open(util.localize(path,
                                                          disco_data=disco_data,
                                                          ddfs_data=ddfs_data)))

    if rest:
        method_name, arg = rest.split('/', 1) if '/' in rest else (rest, None)
        method = getattr(discodb, method_name)
        if method_name in ('metaquery', 'query'):
            return method(Q.urlscan(arg))
        return method(*filter(None, arg))
    return discodb

def input_stream(fd, size, url, params):
    return open(url, task=globals().get('Task')), size, url
