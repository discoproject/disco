
import os, re
from xattr import xattr
from os.path import join

class DFSException(Exception):
        def __init__(self, msg):
                self.msg = msg
        def __str__(self):
                return self.msg

def find_gluster_mountpoint(path):
        is_gluster = lambda p: len([x for x in xattr(p) if "trusted" in x]) > 0
        if not is_gluster(path):
                raise DFSException("%s: Not a Gluster filesystem" % path)
        head = mp = path
        while head != "/" and is_gluster(head):
                mp = head
                head = os.path.split(head)[0]
        return mp

def load_hostname_map(hmap):
        try:
                f = file(os.environ.get("DISCO_HOSTNAME_MAP", hmap))
        except:
                raise DFSException("Hostname map not found at %s "\
                        "(you can specify another file in the "\
                        "DISCO_HOSTNAME_MAP env.var)" % hmap)
        m = {}
        for l in f:
                gluhost, discohost = l.split()
                m[gluhost.strip()] = discohost.strip()
        return m

def replicas(path, hmap):
        r = []
        for x in xattr(path):
                if "trusted" in x:
                        h = x.split(".")[-1]
                        r.append(hmap.get(h, h))
        if r:
                return r
        else:
                return ["unknown"]

def files(path, filter = "", hostname_map = None):
        if type(filter) == str:
                rf = re.compile(".*%s.*" % re.escape(filter))
        else:
                rf = filter
        
        path = os.path.abspath(path)
        mp = find_gluster_mountpoint(path)
        pre = len(mp)

        if not hostname_map:
                hostname_map = load_hostname_map(join(mp, ".hostname_map"))

        inputs = []
        for root, dirs, files in os.walk(path):
                for f in files:
                        p = join(root, f)
                        if not rf.match(p):
                                continue
                        url_path = join(root[pre:], f)
                        if not url_path.startswith("/"):
                                url_path = "/" + url_path
                        urls = ["dfs://%s%s" % (x, url_path)\
                                for x in replicas(p, hostname_map)]
                        inputs.append(urls)
        return inputs


