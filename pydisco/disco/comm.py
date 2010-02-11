import os

import disco.settings

BUFFER_SIZE = int(1024**2)

settings = disco.settings.DiscoSettings()
nocurl = "nocurl" in settings["DISCO_FLAGS"].lower().split()

try:
    import pycurl
except ImportError:
    nocurl = True

if nocurl:
    from disco.comm_httplib import *
else:
    from disco.comm_curl import *

def open_local(path, url):
    # XXX: wouldn't it be polite to give a specific error message if this
    # operation fails
    fd = file(path, "r", BUFFER_SIZE)
    sze = os.stat(path).st_size
    return (fd, sze, "file://" + path)

# get rid of this for python2.6+
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import cjson
        class Dummy(object):
            pass
        json = Dummy()
        json.loads = cjson.decode
        json.dumps = cjson.encode
