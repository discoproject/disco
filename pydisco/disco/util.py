
import re, os, urllib
import sys, time, os, traceback

job_name = "none"

def msg(m, c = 'MSG', job_input = ""):
        t = time.strftime("%y/%m/%d %H:%M:%S")
        print >> sys.stderr, "**<%s>[%s %s (%s)] %s" %\
                (c, t, job_name, job_input, m)

def err(m):
        msg(m, 'MSG')
        raise Exception(m)

def data_err(m, job_input):
        if sys.exc_info() == (None, None, None):
                raise Exception(m)
        else:
                print traceback.print_exc()
                msg(m, 'DAT', job_input)
                raise

def load_conf():
        port = root = master = None
        
        port = (port and port.group(1)) or "8989"
        root = (root and root.group(1)) or "/srv/disco/"
        master = (master and master.group(1)) or port
        
        return os.environ.get("DISCO_MASTER_PORT", master.strip()),\
               os.environ.get("DISCO_PORT", port.strip()),\
               os.environ.get("DISCO_ROOT", root.strip()) + "/data/"


def jobname(addr):
        if addr.startswith("disco:") or addr.startswith("http:"):
                return addr.split("/")[-2]
        elif addr.startswith("dir:"):
                return addr.split("/")[-1]
        else:
                raise "Unknown address: %s" % addr


def external(files):
        msg = {"op": file(files[0]).read()}
        for f in files[1:]:
                msg[os.path.basename(f)] = file(f).read()
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
                raise "Unknown host specifier: %s" % addr


def parse_dir(dir_url, proxy = None):
        x, x, host, mode, name = dir_url.split('/')
        if proxy:
                url = "http://%s/disco/node/%s/%s/" % (proxy, host, name)
        else:
                url = "http://%s:%s/%s/" % (host, HTTP_PORT, name)
        html = urllib.urlopen(url).read()
        inputs = re.findall(">(%s-(.+?)-.*?)</a>" % mode, html)
        return ["%s://%s/%s/%s" % (prefix, host, name, x)\
                        for x, prefix in inputs if "." not in x]

MASTER_PORT, HTTP_PORT, tmp = load_conf()
