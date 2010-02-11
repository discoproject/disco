
import os, re
from xattr import xattr
from os.path import join
from subprocess import Popen, PIPE

class DFSException(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg

def find_gluster_mountpoint(path):
    mount = Popen("mount", stdout = PIPE)
    if mount.wait():
        raise DFSException("Could not execute mount")
    root = ""
    for l in mount.communicate()[0].splitlines():
        # mount output follows the format
        # dxadm|on|/data|type fuse.glusterfs...
        x, x, p, t = l.split(" ", 3)
        if "glusterfs" in t and path.startswith(p)\
            and len(p) > len(root):
            root = p
    if root:
        return root
    raise DFSException("%s: Not a Gluster filesystem" % path)

def load_hostname_map(hmap):
    try:
        f = file(os.environ.get("DISCO_HOSTNAME_MAP", hmap))
    except Exception, e:
        raise DFSException("Hostname map not found at %s "
                   "(you can specify another file in the "
                   "DISCO_HOSTNAME_MAP env.var)" % hmap)
    m = {}
    for l in f:
        gluhost, discohost = l.split()
        m[gluhost.strip()] = discohost.strip()
    return m

def replicas(path, hmap):
    r = []
    attr = xattr(path)
    if "user.glusterfs.location" in attr:
        a = attr["user.glusterfs.location"]
        r = [hmap.get(a, a)]
    elif "trusted.glusterfs.location" in attr:
        a = attr["trusted.glusterfs.location"]
        r = [hmap.get(a, a)]
    else:
        # this mode for gluster prior v.2.0.6
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


