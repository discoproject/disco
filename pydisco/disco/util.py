import os
import sys, time, traceback
from disco.comm import CommException, download, open_remote
from disco.error import DiscoError

job_name = "none"
resultfs_enabled =\
        "resultfs" in os.environ.get("DISCO_FLAGS", "").lower().split()

def msg(m, c = 'MSG', job_input = ""):
        t = time.strftime("%y/%m/%d %H:%M:%S")
        print >> sys.stderr, "**<%s>[%s %s (%s)] %s" %\
                (c, t, job_name, job_input, m)

def err(m):
        msg(m, 'MSG')
        raise DiscoError(m)

def data_err(m, job_input):
        msg(m, 'DAT', job_input)
        if sys.exc_info() == (None, None, None):
                raise DiscoError(m)
        else:
                print traceback.print_exc()
                raise

def load_conf():
        port = root = master = None
        
        port = (port and port.group(1)) or "8989"
        root = (root and root.group(1)) or "/srv/disco/"
        master = (master and master.group(1)) or port
        
        return os.environ.get("DISCO_MASTER_PORT", master.strip()),\
               os.environ.get("DISCO_PORT", port.strip()),\
               os.environ.get("DISCO_ROOT", root.strip())


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
                return "http://%s:%s" % (addr.split("/")[-1], MASTER_PORT)
        elif addr.startswith("http:"):
                return addr
        else:
                raise DiscoError("Unknown host specifier: %s" % addr)

def proxy_url(proxy, path, node = "x"):
        if not proxy:
                proxy = os.environ.get("DISCO_PROXY", None)
        if not proxy:
                return "http://%s:%s/%s" % (node, HTTP_PORT, path)
        if proxy.startswith("disco://"):
                host = "%s:%s" % (proxy[8:], MASTER_PORT)
        elif proxy.startswith("http://"):
                host = proxy[7:]
        else:
                raise DiscoError("Unknown proxy protocol: %s" % proxy)
        return "http://%s/disco/node/%s/%s" % (host, node, path)

def parse_dir(dir_url, proxy = None, part_id = None):
        x, x, host, name = dir_url.split("/", 3)
        url = proxy_url(proxy, name, host)
        if name.endswith(".txt"):
                if resultfs_enabled:
                        r = file("%s/data/%s" % (ROOT, name)).readlines()
                else:
                        r = download(url).splitlines()
        else:
                b, mmax = name.split("/")[-1].split(":")
                fl = len(mmax)
                base = b[:len(b) - fl]
                t = "%s%%.%dd" % (base, fl)
                if part_id != None:
                        r = [t % part_id]
                else:
                        r = [t % i for i in range(int(mmax) + 1)]

        p = "/".join(name.split("/")[:-1])
        return ["disco://%s/%s/%s" % (host, p, x.strip()) for x in r]

def load_oob(host, name, key):
        use_proxy = "DISCO_PROXY" in os.environ
        url = "%s/disco/ctrl/oob_get?name=%s&key=%s&proxy=%d" %\
                (host, name, key, use_proxy)
        if resultfs_enabled:
                sze, fd = open_remote(url, expect = 302)
                loc = fd.getheader("location")
                fname = "%s/data/%s" % (ROOT, "/".join(loc.split("/")[3:]))
                try:
                        return file(fname).read()
                except Exception:
                        raise DiscoError("OOB key (%s) not found at %s" %\
                                 (key, fname))
        else:
                return download(url, redir = True)


MASTER_PORT, HTTP_PORT, ROOT = load_conf()
