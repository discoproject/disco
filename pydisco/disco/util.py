import os
import cPickle, marshal, sys, time, traceback

from collections import defaultdict
from itertools import chain, repeat
from types import CodeType, FunctionType
from urllib import urlencode

from disco.comm import download, open_remote
from disco.error import DiscoError, DataError
from disco.events import Message
from disco.settings import DiscoSettings

class DefaultDict(defaultdict):
    """Like a defaultdict, but calls the default_factory with the key argument."""
    def __missing__(self, key):
        return self.default_factory(key)

class MessageWriter(object):
    def write(self, string):
        Message(string)

def flatten(iterable):
    for item in iterable:
        if isiterable(item):
            for subitem in flatten(item):
                yield subitem
        else:
            yield item

def isiterable(object):
    return hasattr(object, '__iter__')

def iskv(object):
    return isinstance(object, tuple) and len(object) is 2

def iterify(object):
    if hasattr(object, '__iter__'):
        return object
    return repeat(object, 1)

def partition(iterable, fn):
    t, f = [], []
    for item in iterable:
        (t if fn(item) else f).append(item)
    return t, f

def rapply(iterable, fn):
    for item in iterable:
        if isiterable(item):
            yield rapply(item, fn)
        else:
            yield fn(item)

def pack(object):
    if hasattr(object, 'func_code'):
        return marshal.dumps(object.func_code)
    return cPickle.dumps(object)

def unpack(string, globals={}):
    try:
        return cPickle.loads(string)
    except Exception:
        return FunctionType(marshal.loads(string), globals)

def pack_stack(stack):
    return pack([pack(object) for object in stack])

def unpack_stack(stackstring, globals={}):
    return [unpack(string, globals=globals) for string in unpack(stackstring)]

def schemesplit(url):
    return url.split('://', 1) if '://' in url else ('file', url)

def urlsplit(url):
    scheme, rest = schemesplit(url)
    netloc, path = rest.split('/', 1)  if '/'   in rest else (rest ,'')
    if scheme == 'disco':
        scheme = 'http'
        netloc = '%s:%s' % (netloc, DiscoSettings()['DISCO_PORT'])
    return scheme, netloc, path

def urlresolve(url):
    return '%s://%s/%s' % urlsplit(url)

def urllist(url, partid=None, listdirs=True, ddfs=None, recurse=False):
    if isiterable(url):
        return [list(url)]
    scheme, netloc, path = urlsplit(url)
    if scheme == 'dir' and listdirs:
        return parse_dir(url, partid=partid)
    elif scheme == 'tag':
        from disco.ddfs import DDFS
        tag = DDFS(ddfs).get_tag(netloc, recurse=recurse)
        return [repl[0] for repl in tag['urls']]
    return [url]

def msg(message):
    """
    Sends the string *message* to the master for logging. The message is
    shown on the web interface. To prevent a rogue job from overwhelming the
    master, the maximum *message* size is set to 255 characters and job is
    allowed to send at most 10 messages per second.
    """
    return Message(message)

def err(message):
    """Raises an exception with the reason *message*. This terminates the job."""
    raise DiscoError(message)

def data_err(message, url):
    """
    Raises a data error with the reason *message*. This signals the master to re-run
    the task on another node. If the same task raises data error on several
    different nodes, the master terminates the job. Thus data error should only be
    raised if it is likely that the occurred error is temporary.

    Typically this function is used by map readers to signal a temporary failure
    in accessing an input file.
    """
    raise DataError(message, url)

def jobname(address):
    scheme, x, path = urlsplit(address)
    if scheme in ('disco', 'dir', 'http'):
        return path.strip('/').split('/')[-2]
    raise DiscoError("Cannot parse jobname from %s" % address)

def pack_files(files):
    return dict((os.path.basename(f), file(f).read()) for f in files)

def external(files):
    msg = pack_files(files[1:])
    msg['op'] = file(files[0]).read()
    return msg

def proxy_url(path, node='x'):
    settings = DiscoSettings()
    port, proxy = settings['DISCO_PORT'], settings['DISCO_PROXY']
    if proxy:
        scheme, netloc, x = urlsplit(proxy)
        return '%s://%s/disco/node/%s/%s' % (scheme, netloc, node, path)
    return 'http://%s:%s/%s' % (node, port, path)

def parse_dir(dir_url, partid = None):
    def parse_index(index):
        return [url for id, url in (line.split() for line in index)
            if partid is None or partid == int(id)]
    settings = DiscoSettings()
    scheme, netloc, path = urlsplit(dir_url)
    if 'resultfs' in settings['DISCO_FLAGS']:
        path = '%s/data/%s' % (settings['DISCO_ROOT'], path)
        return parse_index(file(path))
    url = proxy_url(path, netloc)
    return parse_index(download(url).splitlines())

def load_oob(host, name, key):
    settings = DiscoSettings()
    params = {'name': name,
          'key': key,
          'proxy': '1' if settings['DISCO_PROXY'] else '0'}
    url = '%s/disco/ctrl/oob_get?%s' % (host, urlencode(params))
    if 'resultfs' in settings['DISCO_FLAGS']:
        size, fd = open_remote(url)
        location = fd.getheader('location').split('/', 3)[-1]
        path = '%s/data/%s' % (settings['DISCO_ROOT'], location)
        return file(path).read()
    return download(url, redir=True)

def ddfs_name(jobname):
    return 'disco:job:results:%s' % jobname

def ddfs_save(blobs, name, master):
    from disco.ddfs import DDFS
    ddfs = DDFS(master)
    blobs = [(blob, ('discoblob:%s:%s' % (name, os.path.basename(blob))))
             for blob in blobs]
    tag, bloburls = ddfs.push(ddfs_name(name), blobs, retries=600)
    return tag
