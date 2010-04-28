import os, re
from urllib import urlencode

from disco.comm import upload, download, json, open_remote
from disco.error import CommError
from disco.settings import DiscoSettings
from disco.util import iterify, partition, urlsplit

unsafe_re = re.compile(r'[^A-Za-z0-9_\-@:]')

class BlobSource(object):
    def __init__(self, source):
        if hasattr(source, 'read'):
            data = source.read()
            self.makefile = lambda: cStringIO.StringIO(data)
            self.size = lambda: len(data)
        else:
            size = os.stat(source).st_size
            self.makefile = lambda: open(source, 'r')
            self.size = lambda: size

def canonizetags(tags):
    return [tagname(tag) for tag in iterify(tags)]

def tagname(tag):
    if isinstance(tag, list):
        if tag:
            return tagname(tag[0])
    elif tag.startswith('tag://'):
        return tag[6:]
    elif '://' not in tag:
        return tag

class DDFS(object):
    def __init__(self, master=None, proxy=None):
        settings    = DiscoSettings()
        self.master = master or settings['DISCO_MASTER']
        self.proxy  = proxy  or settings['DISCO_PROXY']

    @classmethod
    def safe_name(cls, name):
        return unsafe_re.sub('_', name)

    def blobs(self, tag, ignore_missing=True):
        """
        Walks the tag graph starting at `tag`.

        Yields only the terminal nodes of the graph (`blobs`).
        """
        for path, tags, blobs in self.walk(tag, ignore_missing=ignore_missing):
            for replicas in blobs:
                yield replicas

    def delete(self, tag):
        return self._request('/ddfs/tag/%s' % tagname(tag), method='DELETE')

    def exists(self, tag):
        try:
            if open_remote('%s/ddfs/tag/%s' % (self.master, tagname(tag))):
                return True
        except CommError, e:
            if e.code not in (403, 404):
                raise
        return False

    def findtags(self, tags=None, ignore_missing=True):
        import sys
        """
        Finds the nodes of the tag graph starting at `tags`.

        Yields a 3-tuple `(tag, tags, blobs)`.
        """
        seen = set()

        tag_queue = canonizetags(tags)

        for tag in tag_queue:
            if tag not in seen:
                try:
                    urls        = self.get(tag).get('urls', [])
                    tags, blobs = partition(urls, tagname)
                    tags        = canonizetags(tags)
                    yield tag, tags, blobs

                    tag_queue += tags
                    seen.add(tag)
                except CommError, e:
                    if ignore_missing and e.code == 404:
                        tags = blobs = ()
                    else:
                        raise

    def get(self, tag):
        return self._request('/ddfs/tag/%s' % tagname(tag))

    def list(self, prefix=''):
        return self._request('/ddfs/tags/%s' % prefix)

    def pull(self, tag, retries=None):
        """
        Pulls the blobs associated with `tag`.
        """
        pass

    def push(self, tag, files, replicas=None, retries=None):
        """
        Pushes a bunch of files to ddfs and tags them with `tag`.

        `files` can either be a list of paths, (path, name)-tuples, or
        (fileobject, name)-tuples.
        """
        def aim(tuple_or_path):
            if isinstance(tuple_or_path, basestring):
                source = tuple_or_path
                target = self.safe_name(os.path.basename(source))
            else:
                source, target = tuple_or_path
            return BlobSource(source), target

        urls = [self._push(aim(f), replicas, retries) for f in files]
        return self.tag(tag, urls), urls

    def tag(self, tag, urls):
        return self._request('/ddfs/tag/%s' % tagname(tag), json.dumps(urls))

    def walk(self, tag, ignore_missing=True, tagpath=()):
        """
        Walks the tag graph starting at `tag`.

        Yields a 3-tuple `(tagpath, tags, blobs)`.
        """
        try:
            tagpath    += (tagname(tag),)
            urls        = self.get(tag).get('urls', [])
            tags, blobs = partition(urls, tagname)
            tags        = canonizetags(tags)
            yield tagpath, tags, blobs
        except CommError, e:
            if ignore_missing and e.code == 404:
                tags = blobs = ()
            else:
                raise

        for next_tag in tags:
            for child in self.walk(next_tag,
                                   ignore_missing=ignore_missing,
                                   tagpath=tagpath):
                yield child

    def _maybe_proxy(self, url, method='GET'):
        if self.proxy:
            scheme, (host, port), path = urlsplit(url)
            return '%s/proxy/%s/%s/%s' % (self.proxy, host, method, path)
        return url

    def _push(self, (source, target), replicas=None, retries=None, exclude=[]):
        qs = urlencode([(k, v) for k, v in (('exclude', ','.join(exclude)),
                                            ('replicas', replicas)) if v])
        urls = [(url, source)
            for url in self._request('/ddfs/new_blob/%s?%s' % (target, qs))]

        try:
            return [json.loads(url)
                for url in self._upload(urls, retries=retries)]
        except CommError, e:
            scheme, (host, port), path = urlsplit(e.url)
            return self._push((source, target),
                              replicas=replicas,
                              retries=retries,
                              exclude=exclude + [host])

    def _request(self, url, data=None, method=None):
        response = download(self.master + url, data=data, method=method)
        return json.loads(response)

    def _upload(self, urls, retries=10):
        urls = [(self._maybe_proxy(url, method='PUT'), fd) for url, fd in urls]
        return upload(urls, retries=retries)
