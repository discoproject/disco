"""
:mod:`disco.util` --- Helper functions
======================================

This module provides utility functions that are mostly used by Disco
internally.

The :func:`external` function below comes in handy if you use the Disco
external interface.
"""
import os, sys
import cPickle, marshal, time, gzip
import copy_reg, functools

from cStringIO import StringIO
from collections import defaultdict
from itertools import groupby, repeat
from types import CodeType, FunctionType
from urllib import urlencode

from disco.error import DiscoError, DataError, CommError
from disco.events import Message
from disco.settings import DiscoSettings

class DefaultDict(defaultdict):
    """Like a defaultdict, but calls the default_factory with the key argument."""
    def __missing__(self, key):
        return self.default_factory(key)

class MessageWriter(object):
    def write(self, string):
        Message(string.strip())

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
    if isiterable(object):
        return object
    return repeat(object, 1)

def ilen(iter):
    return sum(1 for _ in iter)

def key((k, v)):
    return k

def kvgroup(kviter):
    for k, kvs in groupby(kviter, key):
        yield k, (v for _k, v in kvs)

def kvify(entry):
    return entry if iskv(entry) else ('', entry)

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

def reify(dotted_name, globals=globals()):
    if '.' in dotted_name:
        package, name = dotted_name.rsplit('.', 1)
        return getattr(__import__(package, fromlist=[name]), name)
    return eval(dotted_name, globals)

def argcount(object):
    if hasattr(object, 'func_code'):
        return object.func_code.co_argcount
    argcount = object.func.func_code.co_argcount
    return argcount - len(object.args or ()) - len(object.keywords or ())

def unpickle_partial(func, args, kwargs):
    return functools.partial(unpack(func), *unpack(args), **unpack(kwargs))

def pickle_partial(p):
    return unpickle_partial, (pack(p.func), pack(p.args), pack(p.keywords or {}))

# support functools.partial also on Pythons prior to 3.1
if sys.version_info < (3,1):
    copy_reg.pickle(functools.partial, pickle_partial)

def pack(object):
    if hasattr(object, 'func_code'):
        if object.func_closure != None:
            raise TypeError("Function must not have closures: "\
                            "%s (try using functools.partial instead)"\
                            % object.func_name)
        defs = [pack(x) for x in object.func_defaults]\
                    if object.func_defaults else None
        return marshal.dumps((object.func_code, defs))
    return cPickle.dumps(object, cPickle.HIGHEST_PROTOCOL)

def unpack(string, globals={'__builtins__': __builtins__}):
    try:
        return cPickle.loads(string)
    except Exception:
        try:
            code, defs = marshal.loads(string)
            defs = tuple([unpack(x) for x in defs]) if defs else None
            return FunctionType(code, globals, argdefs = defs)
        except Exception, e:
            raise ValueError("Could not unpack: %s (%s)" % (string, e))

def pack_stack(stack):
    return pack([pack(object) for object in stack])

def unpack_stack(stackstring, globals={}):
    return [unpack(string, globals=globals) for string in unpack(stackstring)]

def schemesplit(url):
    return url.split('://', 1) if '://' in url else ('file', url)

def urlsplit(url, localhost=None, settings=DiscoSettings()):
    scheme, rest = schemesplit(url)
    locstr, path = rest.split('/', 1)  if '/'   in rest else (rest ,'')
    if scheme == 'disco':
        prefix, fname = path.split('/', 1)
        if locstr == localhost:
            scheme = 'file'
            if prefix == 'ddfs':
                path = os.path.join(settings['DDFS_ROOT'], fname)
            else:
                path = os.path.join(settings['DISCO_DATA'], fname)
        else:
            scheme = 'http'
            locstr = '%s:%s' % (locstr, settings['DISCO_PORT'])
    if scheme == 'tag':
        if not path:
            path, locstr = locstr, ''
    return scheme, netloc.parse(locstr), path

def urlresolve(url):
    return '%s://%s/%s' % urlsplit(url)

def urllist(url, partid=None, listdirs=True, ddfs=None):
    from disco.ddfs import DDFS, istag
    if istag(url):
        ret = []
        for name, tags, blobs in DDFS(ddfs).findtags(url):
            ret += blobs
        return ret
    if isiterable(url):
        return [list(url)]
    scheme, netloc, path = urlsplit(url)
    if scheme == 'dir' and listdirs:
        return parse_dir(url, partid=partid)
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

def read_index(dir_url):
    from disco.comm import download
    scheme, netloc, path = urlsplit(dir_url)
    url = proxy_url(path, netloc)
    body = StringIO(download(url))
    if url.endswith(".gz"):
        body = gzip.GzipFile(fileobj = body)
    for line in body:
        id, url = line.split()
        yield int(id), url

def parse_dir(dir_url, partid=None):
    """
    Translates a directory URL to a list of normal URLs.

    This function might be useful for other programs that need to parse
    results returned by :meth:`disco.core.Disco.wait`, for instance.

    :param dir_url: a directory url, such as ``dir://nx02/test_simple@12243344``
    """
    return [url for id, url in read_index(dir_url)
            if partid is None or partid == int(id)]

def save_oob(host, name, key, value):
    from disco.ddfs import DDFS
    DDFS(host).push(ddfs_oobname(name), [(StringIO(value), key)], delayed=True)

def load_oob(host, name, key):
    from disco.ddfs import DDFS
    # NB: this assumes that blobs are listed in LIFO order.
    # We want to return the latest version
    for fd, sze, url in DDFS(host).pull(ddfs_oobname(name),
                                        blobfilter=lambda x: x == key):
        return fd.read()

def ddfs_name(jobname):
    return 'disco:job:results:%s' % jobname

def ddfs_oobname(jobname):
    return 'disco:job:oob:%s' % jobname

def ddfs_save(blobs, name, master):
    from disco.ddfs import DDFS
    ddfs = DDFS(master)
    blobs = [(blob, ('discoblob:%s:%s' % (name, os.path.basename(blob))))
             for blob in blobs]
    tag = ddfs_name(name)
    ddfs.push(tag, blobs, retries=600, delayed=True)
    return "tag://%s" % tag

def format_size(num):
    for unit in ['bytes','KB','MB','GB','TB']:
        if num < 1024.:
            return "%3.1f%s" % (num, unit)
        num /= 1024.
