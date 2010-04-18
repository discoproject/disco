"""
:mod:`disco.util` --- Helper functions
======================================

This module provides utility functions that are mostly used by Disco
internally.

The :func:`external` function below comes in handy if you use the Disco
external interface.
"""
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

class netloc(tuple):
    @classmethod
    def parse(cls, netlocstr):
        if ':' in netlocstr:
            return cls(netlocstr.split(':'))
        return cls((netlocstr, ''))

    def __str__((host, port)):
        return '%s:%s' % (host, port) if port else host

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
    locstr, path = rest.split('/', 1)  if '/'   in rest else (rest ,'')
    if scheme == 'disco':
        scheme = 'http'
        locstr = '%s:%s' % (locstr, DiscoSettings()['DISCO_PORT'])
    return scheme, netloc.parse(locstr), path

def urlresolve(url):
    return '%s://%s/%s' % urlsplit(url)

def urllist(url, partid=None, listdirs=True, ddfs=None):
    if isiterable(url):
        return [list(url)]
    scheme, netloc, path = urlsplit(url)
    if scheme == 'dir' and listdirs:
        return parse_dir(url, partid=partid)
    elif scheme == 'tag':
        from disco.ddfs import DDFS
        ret = []
        for name, tags, blobs in DDFS(ddfs).findtags(url):
            ret += blobs
        return ret
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
    """
    Extracts the job name from an address *addr*.

    This function is particularly useful for using the methods in
    :class:`disco.core.Disco` given only results of a job.
    """
    scheme, x, path = urlsplit(address)
    if scheme in ('disco', 'dir', 'http'):
        return path.strip('/').split('/')[-2]
    raise DiscoError("Cannot parse jobname from %s" % address)

def pack_files(files):
    return dict((os.path.basename(f), open(f).read()) for f in files)

def external(files):
    """
    Packages an external program, together with other files it depends
    on, to be used either as a map or reduce function.

    :param files: a list of paths to files so that the first file points
                  at the actual executable.

    This example shows how to use an external program, *cmap* that needs a
    configuration file *cmap.conf*, as the map function::

        disco.new_job(input=["disco://localhost/myjob/file1"],
                      fun_map=disco.util.external(["/home/john/bin/cmap",
                                                   "/home/john/cmap.conf"]))

    All files listed in *files* are copied to the same directory so any file
    hierarchy is lost between the files. For more information, see :ref:`discoext`.
    """
    msg = pack_files(files[1:])
    msg['op'] = open(files[0]).read()
    return msg

def proxy_url(path, node='x'):
    settings = DiscoSettings()
    port, proxy = settings['DISCO_PORT'], settings['DISCO_PROXY']
    if proxy:
        scheme, netloc, x = urlsplit(proxy)
        return '%s://%s/disco/node/%s/%s' % (scheme, netloc, node, path)
    return 'http://%s:%s/%s' % (node, port, path)

def parse_dir(dir_url, partid = None):
    """
    Translates a directory URL to a list of normal URLs.

    This function might be useful for other programs that need to parse
    results returned by :meth:`disco.core.Disco.wait`, for instance.

    :param dir_url: a directory url, such as ``dir://nx02/test_simple@12243344``

    :param proxy: address of a proxy server, typically the Disco master's.
                  The addresses are translated to point at the proxy,
                  and prefixed with ``/disco/node/``.
                  The address of a proxy server is typically specified in the
                  :ref:`settings` ``DISCO_PROXY``.
    """
    def parse_index(index):
        return [url for id, url in (line.split() for line in index)
            if partid is None or partid == int(id)]
    settings = DiscoSettings()
    scheme, netloc, path = urlsplit(dir_url)
    if 'resultfs' in settings['DISCO_FLAGS']:
        path = '%s/data/%s' % (settings['DISCO_ROOT'], path)
        return parse_index(open(path))
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
        return open(path).read()
    return download(url, redir=True)

def ddfs_name(jobname):
    return 'disco:job:results:%s' % jobname

def ddfs_save(blobs, name, master):
    from disco.ddfs import DDFS
    ddfs = DDFS(master)
    blobs = [(blob, ('discoblob:%s:%s' % (name, os.path.basename(blob))))
             for blob in blobs]
    tag = ddfs_name(name)
    ddfs.push(tag, blobs, retries=600)
    return "tag://%s" % tag

