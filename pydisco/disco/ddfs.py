import os, re
from urllib import urlencode

from disco import util
from disco.comm import upload, download, json
from disco.error import CommError
from disco.settings import DiscoSettings

unsafe_re = re.compile(r'[^A-Za-z0-9_\-@:]')

def tagname(tag):
    if isinstance(tag, list):
        return tagname(tag[0])
    if tag.startswith('tag://'):
        return tag[6:]
    if '://' not in tag:
        return tag

def netlocsplit(netloc):
    if ':' in netloc:
        return netloc.split(':')
    return netloc, ''

def urlsplit(url):
    scheme, netloc, path = util.urlsplit(url)
    return scheme, netlocsplit(netloc), path

class DDFS(object):
    def __init__(self, master=None, proxy=None):
        settings    = DiscoSettings()
        self.master = master or settings['DISCO_MASTER']
        self.proxy  = proxy  or settings['DISCO_PROXY']

    def pull(self, tag, retries=None):
        """
        Pulls the blobs associated with `tag`.
        """
        pass

    def push(self, tag, files, replicas=None, retries=None):
        """
        Pushes a bunch of files to ddfs and tags them with `tag`.

        `files` can either be a list of paths or (path, name)-tuples.
        """
        def aim(tuple_or_path):
            if isinstance(tuple_or_path, basestring):
                source = tuple_or_path
                return source, unsafe_re.sub('_', os.path.basename(source))
            source, target = tuple_or_path
            return source, target

        urls = [self._push(aim(f), replicas, retries) for f in files]
        return self.tag(tag, urls), urls

    def tag(self, tag, urls):
        return self._request('/ddfs/tag/%s' % tag, json.dumps(urls))

    def get(self, tag):
        return self._request('/ddfs/tag/%s' % tagname(tag), default='{}')

    def blobs(self, tag, ignore_missing=True):
        """
        Walks the tag graph starting at `tag`.

        Yields only the terminal nodes of the graph (`blobs`).
        """
        for path, tags, blobs in self.walk(tag, ignore_missing=ignore_missing):
            for replicas in blobs:
                yield replicas

    def walk(self, tag, ignore_missing=True, seen=set(), tagpath=()):
        """
        Walks the tag graph starting at `tag`.

        Yields a 3-tuple `(tagpath, tags, blobs)`.
        """
        try:
            tagpath    += (tag,)
            urls        = self.get(tag).get('urls', [])
            tags, blobs = util.partition(urls, tagname)
            yield tagpath, tags, blobs
        except CommError, e:
            if ignore_missing and e.code == 404:
                tags = blobs = ()
            else:
                raise

        for urls in tags:
            next_tag = tagname(urls)
            if next_tag not in seen:
                seen = seen | set([next_tag])
                for child in self.walk(next_tag,
                                       ignore_missing=ignore_missing,
                                       seen=seen,
                                       tagpath=tagpath):
                    yield child

    def list(self, prefix=''):
        return self._request('/ddfs/tags/%s' % prefix)

    def delete(self, tag):
        return self._request('/ddfs/tag/%s' % tag, method='DELETE')

    def _push(self, (source, target), replicas=None, retries=None, exclude=[]):
        qs = urlencode((k, v) for k, v in (('exclude', ','.join(exclude)),
                                           ('replicas', replicas)) if v)
        dsts = self._request('/ddfs/new_blob/%s?%s' % (target, qs))
        try:
            return [json.loads(url)
                    for url in self._upload(fname, dsts, retries=retries)]
        except CommError, e:
            scheme, (host, port), path = urlsplit(e.url)
            return self._push((source, target),
                              replicas=replicas,
                              retries=retries,
                              exclude=exclude + [host])

    def _maybe_proxy(self, url, method='GET'):
        if self.proxy:
            scheme, (host, port), path = urlsplit(url)
            return '%s/proxy/%s/%s/%s' % (self.proxy, host, method, path)
        return url

    def _upload(self, filename, urls, retries=10):
        urls = [self._maybe_proxy(url, method='PUT') for url in urls]
        return upload(filename, urls, retries=retries)

    def _request(self, url, data=None, method=None, default='[]'):
        response = download(self.master + url, data=data, method=method)
        return json.loads(response or default)
