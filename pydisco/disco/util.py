import os
import sys, time, traceback
from disco.comm import CommException, download, open_remote
from disco.error import DiscoError
from disco.settings import DiscoSettings


def msg(m, c = 'MSG', job_input = ""):
        t = time.strftime("%y/%m/%d %H:%M:%S")
        if job_input:
                print >> sys.stderr, "**<%s>[%s (%s)] %s" %\
                        (c, t, job_input, m)
        else:
                print >> sys.stderr, "**<%s>[%s] %s" % (c, t, m)

def err(m):
        if sys.exc_info() == (None, None, None):
                msg(m, 'MSG')
                raise DiscoError(m)
        else:
                msg(m, 'MSG')
                raise

def data_err(m, job_input):
        if sys.exc_info() == (None, None, None):
                msg(m, 'DAT', job_input)
                raise DiscoError(m)
        else:
                traceback.print_exc()
                msg(m, 'DAT', job_input)
                raise

def jobname(addr):
        if addr.startswith("disco:") or addr.startswith("http:"):
                return addr.strip("/").split("/")[-2]
        elif addr.startswith("dir:"):
                return addr.strip("/").split("/")[-2]
        else:
                raise DiscoError("Unknown address: %s" % addr)

def pack_files(files):
        msg = {}
        for f in files:
                msg[os.path.basename(f)] = file(f).read()
        return msg

def external(files):
        msg = pack_files(files[1:])
        msg["op"] = file(files[0]).read()
        return msg

def disco_host(addr):
        if addr.startswith("disco:"):
                addr = addr.split("/")[-1]
                if ":" in addr:
                        addr = addr.split(":")[0]
                        print >> sys.stderr, "NOTE! disco://host:port format "\
                                "is deprecated.\nUse disco://host instead, or "\
                                "http://host:port if master doesn't run at "\
                                "DISCO_PORT."
                return "http://%s:%d" % (addr.split("/")[-1],
                                DiscoSettings()["DISCO_PORT"])
        elif addr.startswith("http:"):
                return addr
        else:
                raise DiscoError("Unknown host specifier: %s" % addr)

def proxy_url(proxy, path, node = "x"):
        port = DiscoSettings()["DISCO_PORT"]
        if not proxy:
                proxy = DiscoSettings()["DISCO_PROXY"]
        if not proxy:
                return "http://%s:%s/%s" % (node, port, path)
        if proxy.startswith("disco://"):
                host = "%s:%s" % (proxy[8:], port)
        elif proxy.startswith("http://"):
                host = proxy[7:]
        else:
                raise DiscoError("Unknown proxy protocol: %s" % proxy)
        return "http://%s/disco/node/%s/%s" % (host, node, path)

def parse_dir(dir_url, proxy = None, partid = None):
        def parse_index(f):
                r = []
                for l in f:
                        id, url = l.strip().split()
                        if partid and partid != int(id):
                                continue
                        r.append(url)
                return r
        conf = DiscoSettings()
        x, x, host, name = dir_url.split("/", 3)
        if "resultfs" in conf["DISCO_FLAGS"]:
                root = conf["DISCO_ROOT"]
                return parse_index(file("%s/data/%s" % (root, name)))
        else:
                url = proxy_url(proxy, name, host)
                return parse_index(download(url).splitlines())

def load_oob(host, name, key):
        conf = DiscoSettings()
        use_proxy = conf["DISCO_PROXY"] != ""
        url = "%s/disco/ctrl/oob_get?name=%s&key=%s&proxy=%d" %\
                (host, name, key, use_proxy)
        if resultfs_enabled:
                root = conf["DISCO_ROOT"]
                sze, fd = open_remote(url, expect = 302)
                loc = fd.getheader("location")
                fname = "%s/data/%s" % (root, "/".join(loc.split("/")[3:]))
                try:
                        return file(fname).read()
                except KeyboardInterrupt:
                        raise
                except Exception:
                        raise DiscoError("OOB key (%s) not found at %s" %\
                                 (key, fname))
        else:
                return download(url, redir = True)


