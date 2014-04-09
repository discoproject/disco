from disco import util
from discodb import DiscoDB, Q
from disco.worker.task_io import task_output_stream

def Open(url, task=None):
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
    discodb = DiscoDB.load(open(util.localize(path, disco_data=disco_data,
                                ddfs_data=ddfs_data)))

    if rest:
        method_name, arg = rest.split('/', 1) if '/' in rest else (rest, None)
        method = getattr(discodb, method_name)
        if method_name in ('metaquery', 'query'):
            return method(Q.urlscan(arg))
        return method(*filter(None, arg))
    return discodb

def input_stream(fd, size, url, params):
    return Open(url, task=globals().get('Task')), size, url

class DiscoDBOutput(object):
    def __init__(self, stream, params):
        from discodb import DiscoDBConstructor
        self.discodb_constructor = DiscoDBConstructor()
        self.stream = stream
        self.params = params
        self.path = stream.path

    def add(self, key, val):
        self.discodb_constructor.add(key, val)

    def close(self):
        def flags():
            return dict((flag, getattr(self.params, flag))
                        for flag in ('unique_items', 'disable_compression')
                        if hasattr(self.params, flag))
        self.discodb_constructor.finalize(**flags()).dump(self.stream)

def discodb_output(stream, partition, url, params):
    return DiscoDBOutput(stream, params), 'discodb:{0}'.format(url.split(':', 1)[1])

discodb_stream = (task_output_stream, discodb_output)
