import os

nocurl = "nocurl" in os.environ.get("DISCO_FLAGS", "").lower().split()

try:
        import pycurl
except:
        nocurl = True

if nocurl:
        from comm_httplib import *
else:
        from comm_curl import *

