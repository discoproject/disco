
import os, re
from disco import error
from disco.util import disco_host, urlsplit
from disco.comm import upload, download, json
from disco.error import CommError
from disco.settings import DiscoSettings

class DDFS(object):
    def __init__(self, host):
        proxy = DiscoSettings()["DISCO_PROXY"]
        if proxy == "":
            self.host = disco_host(host)
            self.proxy = False
        else:
            scheme, host, path = urlsplit(proxy)
            self.host = "http://" + host
            self.proxy = True

    def put(self, tag, files, replicas = None, retries = None, return_tag_urls = False):
        urls = [self._put_file(fname, replicas, retries) for fname in files]
        return self.tag(tag, urls, return_tag_urls = return_tag_urls), urls

    def tag(self, tag, urls, return_tag_urls = False):
        tag_urls = self._request("/ddfs/tag/" + tag, json.dumps(urls))
        if return_tag_urls:
            return tag_urls
        else:
            return "tag://%s" % tag

    def get_tag(self, tag, recurse = False,
                    ignore_missing = True, ignore = {}):
        if tag.startswith("tag://"):
            tag = tag[6:]
        tag = self._request("/ddfs/tag/" + tag)
        if recurse:
            res = []
            for repl in tag['urls']:
                if len(repl) == 1 and repl[0].startswith("tag://"):
                    ntag = repl[0][6:]
                    if ntag in ignore:
                        continue
                    ignore[ntag] = True
                    try:
                        res += self.get_tag(ntag,
                            recurse = True, ignore = ignore)
                    except error.CommError, x:
                        if ignore_missing and x.code == 404:
                            continue
                        raise
                else:
                    res.append(repl)
            return res
        else:
            return tag

    def delete_tag(self, tag):
        return self._request("/ddfs/tag/" + tag, method = "DELETE")

    def tags(self):
        return self._request("/ddfs/tags")

    def _put_file(self, blob, replicas, retries, exclude = []):
        if type(blob) == tuple:
            name, fname = blob
        else:
            fname = blob
            name = re.sub("[^A-Za-z0-9_\-@:]", "_", os.path.basename(fname))
        qs = "?exclude=" + ",".join(exclude)
        if replicas:
            qs += "&replicas=%d" % replicas
        dst = self._request("/ddfs/new_blob/%s%s" % (name, qs))
        if self.proxy:
            proxied = []
            for url in dst:
                scheme, host, path = urlsplit(url)
                host = host.split(":")[0]
                proxied.append("%s/proxy/%s/PUT/%s" %\
                    (self.host, host, path))
            dst = proxied
        try:
            if retries != None:
                urls = upload(fname, dst, retries = retries)
            else:
                urls = upload(fname, dst)
        except CommError, ex:
            scheme, host, path = urlsplit(ex.url)
            host = host.split(':')[0]
            exclude.append(host)
            return self._put_file(blob, replicas, retries, exclude)
        return [json.loads(url) for url in urls]

    def _request(self, url, data = None, method = None):
        return json.loads(download(self.host + url,
                    data = data, method = method))

if __name__ == "__main__":
    print DDFS("http://localhost:8989").put("proxtest", ["testfile.big"])
