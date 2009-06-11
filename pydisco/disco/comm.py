import os

try:
        import pycurl
except:
        nocurl = True

nocurl = "nocurl" in os.environ.get("DISCO_FLAGS", "").lower().split()

if nocurl:
        from comm_httplib import *
else:
        from comm_curl import *

